/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // Message's MAGIC CODE daa320a7
    // Message 的魔数，用于在校验消息时进行判断，确保是合法的消息。
    public final static int MESSAGE_MAGIC_CODE = -626843481;

    // End of file empty MAGIC CODE cbd43194
    // 表示文件结尾的空白魔数，用于在文件写入时判断是否到达文件末尾。
    protected final static int BLANK_MAGIC_CODE = -875286124;

    // 文件队列，用于存储 CommitLog 文件，实现了文件的快速读写、磁盘空间管理和清理功能。
    protected final MappedFileQueue mappedFileQueue;
    // 相互持有。 消息存储服务的实例，主要用于消息读取和消费进度的更新。
    protected final DefaultMessageStore defaultMessageStore;

    // 刷盘服务，用于将内存中的消息数据刷写到磁盘文件中。
    private final FlushCommitLogService flushCommitLogService;

    //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    // 刷盘服务，主要用于支持瞬态存储池，将消息数据从瞬态存储池中刷写到磁盘中。
    private final FlushCommitLogService commitLogService;

    // 追加消息的回调接口，主要用于异步刷盘时回调，通知业务层消息已经成功追加到 CommitLog 中。
    private final AppendMessageCallback appendMessageCallback;

    // 线程本地变量，用于保存批量消息的编码器，避免在高并发场景下出现线程安全问题。
    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;

    // 用于保存消息在 CommitLog 中的 offset，键为消息的 topic-queueId，值为 offset。
    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);

    // 用于保存消息消费的进度，即消费者消费到哪个位置的消息。
    protected volatile long confirmOffset = -1L;

    // 用于记录进入 CommitLog 锁的时间，用于计算锁的等待时间。
    private volatile long beginTimeInLock = 0;
    // CommitLog 锁，用于保证消息追加的线程安全性。
    protected final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {

        // 创建文件队列
        // 参数：存储路径、CommitLog 文件的大小，默认为 1G、MappedFile 分配服务 （内存映射技术）
        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
                defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());

        // 相互持有
        this.defaultMessageStore = defaultMessageStore;

        // 根据 FlushDiskType 创建不同的 flushCommitLogService，用于定时刷盘。
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        // 创建 CommitRealTimeService，用于将消息追加到 CommitLog 文件中。
        this.commitLogService = new CommitRealTimeService();

        // 创建 DefaultAppendMessageCallback，用于将消息追加到 MappedFile 中。
        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

        // 创建 batchEncoderThreadLocal，用于将消息编码为批量消息。
        batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
            @Override
            protected MessageExtBatchEncoder initialValue() {
                // 参数：消息的最大大小，默认为4M
                return new MessageExtBatchEncoder(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };

        // 在将消息放入队列时，是否使用互斥锁。默认为 false，表示使用自旋锁
        // 创建 putMessageLock，用于控制消息追加时的并发操作。可以选择使用 ReentrantLock 或 SpinLock
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {

        // 启动刷盘服务，用于将内存中的消息数据刷写到磁盘文件中。
        this.flushCommitLogService.start();

        //  启动刷盘服务，主要用于支持瞬态存储池，将消息数据从瞬态存储池中刷写到磁盘中。
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }


    /**
     * 删除过期的文件
     *
     * @param expiredTime         日志文件保留时间，eg：CommitLog 文件默认配置为 72 小时， 但是在调用时，已经将 72小时 转换成了 毫秒数 ！！！
     * @param deleteFilesInterval 删除文件间隔时间，单位为毫秒，表示每删除一个文件后暂停的时间。
     * @param intervalForcibly    强制删除间隔时间，单位为毫秒，表示每次尝试删除文件的间隔时间。
     * @param cleanImmediately    是否立即清理文件，若为true，则无论是否过期都会立即删除。
     * @return 删除的文件数
     */
    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        // 获取commitLog文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 根据给定的偏移量查找对应的 MappedFile，
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            // 计算在 MappedFile 中的偏移量 (然后用offset与文件长度取余，得到在文件内的偏移量)
            int pos = (int) (offset % mappedFileSize);
            //  查找 pos到当前最大可读指针之间的数据
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            // 截断 ConsumeQueue 的逻辑文件，用于清理消费队列中 过期或者脏数据 消息，
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        } else {
            // Commitlog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();

            int queueId = byteBuffer.getInt();

            int flag = byteBuffer.getInt();

            long queueOffset = byteBuffer.getLong();

            long physicOffset = byteBuffer.getLong();

            int sysFlag = byteBuffer.getInt();

            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            long storeTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            int reconsumeTimes = byteBuffer.getInt();

            long preparedTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (ScheduleMessageService.SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                    storeTimestamp);
                        }
                    }
                }
            }

            int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                        "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                        totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            return new DispatchRequest(
                    topic,
                    queueId,
                    physicOffset,
                    totalSize,
                    tagsCode,
                    storeTimestamp,
                    queueOffset,
                    keys,
                    uniqKey,
                    sysFlag,
                    preparedTransactionOffset,
                    propertiesMap
            );
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    /**
     * 计算消息长度
     *
     * @param sysFlag          系统标志
     * @param bodyLength       消息体长度
     * @param topicLength      主题长度
     * @param propertiesLength 消息属性长度
     * @return 消息长度
     */
    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        // IPV4 or IPV6
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        final int msgLen = 4 //TOTALSIZE    消息条目总产度，4字节
                + 4 //MAGICCODE             魔法数，4字节。固定值 0xdaa320a7
                + 4 //BODYCRC               消息体的crc校验码，4字节。
                + 4 //QUEUEID               消息消费队列ID，4字节。
                + 4 //FLAG                  消息标记，RocketMQ对其不做处理，供应用程序使用，默认4字节。
                + 8 //QUEUEOFFSET           消息在ConsumeQuene文件中的物理偏移量，8字节。
                + 8 //PHYSICALOFFSET        消息在CommitLog文件中的物理偏移量，8字节。
                + 4 //SYSFLAG               消息系统标记，例如是否压缩、是否是事务消息等，4字节。
                + 8 //BORNTIMESTAMP         消息生产者调用消息发送API的时间戳，8字节。
                + bornhostLength //BORNHOST 消息发送者IP、端口号，8字节。
                + 8 //STORETIMESTAMP        消息存储时间戳，8字节。
                + storehostAddressLength //STOREHOSTADDRESS         Broker服务器IP+端口号，8字节。
                + 4 //RECONSUMETIMES                                消息重试次数，4字节。
                + 8 //Prepared Transaction Offset                   事务消息的物理偏移量，8字节。
                + 4 //                                              消息体长度，4字节。
                + (bodyLength > 0 ? bodyLength : 0) //BODY          消息体内容，长度为bodyLenth中存储的值。
                + 1 //                                              主题存储长度，1字节，表示主题名称不能超过255个字符。
                + topicLength //TOPIC                               主题，长度为TopicLength中存储的值。
                + 2 //                                              消息属性长度，2字节，表示消息属性长度不能超过65536个字符。
                + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength    消息属性，长度为PropertiesLength中存储的值。
                + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        mappedFileOffset += size;

                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                            if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                                this.defaultMessageStore.doDispatch(dispatchRequest);
                            }
                        } else {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    else if (size == 0) {
                        index++;
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {
                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            // 截断 ConsumeQueue 的逻辑文件，用于清理消费队列中 过期或者截断的脏数据 的消息，
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
        long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
        if (0 == storeTimestamp) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }


    /**
     * 存储消息
     *
     * @param msg 消息
     * @return PutMessageResult
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {

        // 设置消息存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());
        // 设置消息体的校验码
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        // 存储统计服务
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 主题
        String topic = msg.getTopic();
        // 队列ID
        int queueId = msg.getQueueId();

        // 从消息标志位中获取事务类型
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        // 非事务消息 或者 表示事务消息的 Commit 状态。
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            // 检查消息是否需要延迟投递，如果是则将主题和队列ID修改为调度主题和调度队列ID，并备份实际主题和队列ID；
            if (msg.getDelayTimeLevel() > 0) {

                // 验证延迟级别是否超出上限
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                // 将主题修改为调度主题
                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                // 将队列ID修改为调度队列ID
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // 备份实际主题、队列ID
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                // 更新消息属性值
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                // 更新消息 主题、队列ID
                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        // IPV6兼容：记录消息发送端的地址；
        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        // IPV6兼容：记录存储端的地址；
        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        // 用于记录上锁所消耗的时间
        long eclipsedTimeInLock = 0;

        MappedFile unlockMappedFile = null;
        // 获取最后一个映射文件，作为消息的写入目标
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 加锁保证同一时刻只有一个线程可以操作CommitLog
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {

            // 锁开始时间
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            // 设置消息的存储时间戳，确保全局顺序
            msg.setStoreTimestamp(beginLockTimestamp);

            // 如果 mappedFile 为空，则表明没有则需要创建。 或者 lastMappedFile 已经满了，也需要创建新的 MappedFile。
            if (null == mappedFile || mappedFile.isFull()) {
                // 获取最后一个MappedFile对象。如果需要创建新的MappedFile，则创建并返回新的对象，否则返回最后一个MappedFile对象
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }

            // 如果 mappedFile 还为空，则返回 创建映射文件失败。
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            // 核心: 将消息追加到文件中
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            // 根据追加消息的结果进行相应的处理
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE: // 如果文件末尾，新建文件后重写消息
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    // 重写消息
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED: // 如果消息过大或属性过多，则返回消息非法
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR: // 如果出现未知错误，则返回未知错误；
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            // 记录上锁所消耗的时间
            eclipsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            // 释放锁
            putMessageLock.unlock();
        }

        // 超过 500ms 打印 warn日志。
        if (eclipsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        // 添加到文件成功
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 统计消息投递次数
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        // 统计消息投递大小；
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        // 核心：处理刷盘
        handleDiskFlush(result, putMessageResult, msg);

        // 处理高可用
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }

    /**
     * 处理刷盘操作
     *
     * @param result           消息追加结果
     * @param putMessageResult 消息PUT结果
     * @param messageExt       消息
     */
    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {

            // 类型转换，GroupCommitService 为同步刷盘处理类
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            // 是否存储成功
            if (messageExt.isWaitStoreMsgOK()) {
                // 创建同步刷盘请求
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                // 提交请求到 同步刷盘任务处理器
                service.putRequest(request);
                // 同步阻塞等待刷盘结果
                boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                            + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else {
                service.wakeup();
            }
        }
        // 异步刷盘
        else {
            // 不开启瞬态存储池
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                // 用于将内存中的消息数据刷写到磁盘文件中。
                flushCommitLogService.wakeup();
            } else {
                // 要用于支持瞬态存储池，将消息数据从瞬态存储池中刷写到磁盘中。
                commitLogService.wakeup();
            }
        }
    }

    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // Determine whether to wait
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    service.putRequest(request);
                    service.getWaitNotifyObject().wakeupAll();
                    boolean flushOK =
                            request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: "
                                + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

    }

    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long eclipsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (eclipsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        handleDiskFlush(result, putMessageResult, messageExtBatch);

        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     *
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */


    /**
     * 该方法用于获取指定消息在 CommitLog 中的存储时间戳。如果发生错误，则返回 -1。
     *
     * @param offset 指定消息在 CommitLog 中的物理偏移量
     * @param size   指定消息的大小
     * @return 存储时间戳，如果发生错误则返回 -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {

        // 先判断是否大于 当前CommitLog目录的最小偏移量
        if (offset >= this.getMinOffset()) {
            // 根据指定的偏移量和大小，从 CommitLog 中获取对应的消息
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    // 从消息的系统标志中获取 BornHost 的长度
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;

                    // 计算消息存储时间戳的位置，因为 BORNHOST 后面就是 消息存储的时间戳 ！！！
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    // 从消息中获取存储时间戳并返回
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    // 释放 SelectMappedBufferResult 对象
                    result.release();
                }
            }
        }

        return -1;
    }

    /**
     * 获取当前CommitLog目录的最小偏移量
     *
     * @return long
     */
    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            // 如果该文件可用，则返回该文件的起始偏移量，否则返回下一个文件的起始偏移量
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }


    /**
     * 根据偏移量、大小 获取消息数据
     *
     * @param offset commitLogOffset 物理偏移量
     * @param size   字节大小
     * @return SelectMappedBufferResult
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        // 获取commitLog文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 根据给定的偏移量查找对应的 MappedFile，
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            // 计算在 MappedFile 中的偏移量 (然后用offset与文件长度取余，得到在文件内的偏移量)
            int pos = (int) (offset % mappedFileSize);
            // 查找从 pos ~ pos+size 之间的数据
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * 追加数据
     *
     * @param startOffset 起始偏移量
     * @param data        数据
     * @return boolean
     */
    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            // 获取最后一个 MappedFile
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            // 追加数据
            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }


    // ================================ 内部类 =======================================


    /**
     * 刷盘抽象服务
     */
    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 要用于支持瞬态存储池，将消息数据从瞬态存储池中刷写到磁盘中。
     */
    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                int commitDataThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread.
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 异步刷盘
     * <p>
     * 异步刷盘有两种方式，但是其逻辑都是需要刷盘的数据OS_PAGE_SIZE的4倍即（1024 * 4）*4=16k 或者 距上一次刷盘时间>=200ms时才刷盘，提高数据的刷盘性能
     */
    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {

                //  是否定时刷新 CommitLog，默认为实时刷新。
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                // CommitLog 刷新磁盘的间隔时间，默认为 500ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                // 刷盘CommitLog时的最小刷盘页数，默认为4
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                // 刷盘CommitLog时的强制刷盘间隔，默认为10000ms（10秒）。
                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    // 刷盘
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    // 刷盘存储时间戳
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        // 更新 checkpoint 文件
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile boolean flushOK = false;

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        /**
         * 唤醒对应阻塞线程
         *
         * @param flushOK 刷盘状态
         */
        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }

        public boolean waitForFlush(long timeout) {
            try {
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return this.flushOK;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
                return false;
            }
        }
    }

    /**
     * 同步刷盘
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                            if (!flushOK) {
                                CommitLog.this.mappedFileQueue.flush(0);
                            }
                        }

                        req.wakeupCustomer(flushOK);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                } else {
                    // Because of individual messages is set to not sync flush, it
                    // will come to this process
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }


    /**
     * DefaultAppendMessageCallback是一个实现了AppendMessageCallback接口的默认回调方法，
     * 用于实现消息追加到MappedByteBuffer中的具体逻辑。
     */
    class DefaultAppendMessageCallback implements AppendMessageCallback {

        /**
         * File at the end of the minimum fixed length empty （文件末尾的最小固定长度为空）
         * <p>
         * 在 RocketMQ 的存储架构中，每个 CommitLog 文件的末尾都有一定的空间预留出来，用于下次写入消息。这个预留空间的大小就是 END_FILE_MIN_BLANK_LENGTH。
         * 这个常量的值是 8，即 4 字节的 int 类型的消息长度和 4 字节的 int 类型的消息的魔数（0xdaa320a7）。
         * <p>
         * 当写入消息时，如果当前 CommitLog 文件的剩余空间不足以存储消息和 END_FILE_MIN_BLANK_LENGTH，那么就需要切换到下一个文件来写入消息。
         */
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

        // 用于存储消息 ID 的 ByteBuffer。
        private final ByteBuffer msgIdMemory;
        // 用于存储消息 ID V6 的 ByteBuffer。
        private final ByteBuffer msgIdV6Memory;
        // 用于存储消息内容的 ByteBuffer。
        private final ByteBuffer msgStoreItemMemory;

        // 消息的最大大小，默认为4M
        private final int maxMessageSize;


        // 用于构建消息 Key 的 StringBuilder。
        private final StringBuilder keyBuilder = new StringBuilder();
        // 用于构建消息 ID 的 StringBuilder。
        private final StringBuilder msgIdBuilder = new StringBuilder();

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }


        /**
         * 单条消息
         * 消息序列化后，写入 MapedByteBuffer （即写入 mmap 映射文件，然后等待 flush 刷盘）
         *
         * @param fileFromOffset 表示要写入的映射文件缓冲区的起始偏移量。
         * @param byteBuffer     MappedFile 持有的 MappedByteBuffer 对象调用slice方法 返回的新的 ByteBuffer 对象。
         * @param maxBlank       表示当前映射文件缓冲区中未被使用的最大空间大小。
         * @param msgInner       表示待写入的消息对象，它是一个 MessageExtBrokerInner 类型的对象。
         * @return AppendMessageResult
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // 文件写入位置（PHY OFFSET）
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 系统标志
            int sysflag = msgInner.getSysFlag();

            // 通过判断是否为 IPV6 地址，计算 bornHost、storeHost 长度。
            int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 分配内容
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);
            // 重置
            this.resetByteBuffer(storeHostHolder, storeHostLength);

            // 创建消息ID
            String msgId;
            if ((sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                // IPV4
                msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(storeHostHolder), wroteOffset);
            } else {
                // IPV6
                msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, msgInner.getStoreHostBytes(storeHostHolder), wroteOffset);
            }

            // 构建key，从获topicQueueTable中 获取该消息在消息队列的物理偏移量
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            // 不存在写入
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // 需要特殊处理的事务消息
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L; // 事物消息设置为0
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /**
             * 开始序列化消息
             */

            // 消息属性、消息属性长度
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;
            // 消息属性长度 超过限制，则结束返回 PROPERTIES_SIZE_EXCEEDED。
            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            // 主题、主题名称长度
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;
            // 消息体长度
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            // 计算消息长度
            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message
            // 判断消息大小是否超过限制 4MB
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Determines whether there is sufficient free space
            // 判断 当前消息大小 + 预留空间(8字节) 是否大于 当前映射文件(MappedFile, mmap) 剩下可写的字节数
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                // 返回 END_OF_FILE，上层会创建 MappedFile ，然后再次尝试存储消息。
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                        queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            // 初始化存储空间
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            this.resetByteBuffer(bornHostHolder, bornHostLength);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.resetByteBuffer(storeHostHolder, storeHostLength);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
            // 13 RECONSUMETIMES
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData);

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            // 是将消息存储在MappedFile对应的内存映射Buffer中，并没有写入磁盘
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            // 组装返回结果
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                    msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBatch messageExtBatch) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;
            msgIdBuilder.setLength(0);
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            this.resetByteBuffer(storeHostHolder, storeHostLength);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(storeHostHolder);
            messagesByteBuff.mark();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                storeHostBytes.rewind();
                String msgId;
                if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                    msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                } else {
                    msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                }

                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        /**
         * 用于重置一个 ByteBuffer 对象，让它的 position 属性变为 0，并将 limit 属性设置为指定的值
         *
         * @param byteBuffer ByteBuffer 对象
         * @param limit      指定的值
         */
        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    /**
     * 批量消息的编码器
     */
    public static class MessageExtBatchEncoder {
        // Store the message content
        private final ByteBuffer msgBatchMemory;
        // The maximum length of the message
        private final int maxMessageSize;

        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            msgBatchMemory.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, propertiesLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET
                this.msgBatchMemory.putLong(0);
                // 7 PHYSICALOFFSET
                this.msgBatchMemory.putLong(0);
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(bornHostHolder, bornHostLength);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(bornHostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(storeHostHolder, storeHostLength);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort(propertiesLen);
                if (propertiesLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}
