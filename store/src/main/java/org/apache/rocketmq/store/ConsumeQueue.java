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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * RocketMQ基于主题订阅模式实现消息消费，消费者关心的是一个主题下的所有消息，但同一主题的消息是不连续地存储在CommitLog文件中的。
 * 如果消息消费者直接从消息存储文件中遍历查找订阅主题下的消息，效率将极其低下。RocketMQ为了适应消息消费的检索需求，
 * 设计了ConsumeQueue文件，该文件可以看作CommitLog关于消息消费的“索引”文件，ConsumeQueue的第一级目录为消息主题，第二级目录为主题的消息队列
 */
public class ConsumeQueue {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // ConsumeQueue 条目大小 20字节
    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    // 消息存储默认实现，相互持有
    private final DefaultMessageStore defaultMessageStore;

    // 文件队列
    private final MappedFileQueue mappedFileQueue;
    // 主题
    private final String topic;
    // 队列ID
    private final int queueId;

    // consumeQueue 队列中每个条目大小 = commitLogOffset(8) + msgSize(4) + tagCode(8)  = 20字节。
    // 主要用于临时存储 单个条目数据，然后写入映射文件。
    private final ByteBuffer byteBufferIndex;

    // 存储路径
    private final String storePath;
    // 映射文件大小，默认单个文件的长度为 3×100000 (30w个条目) × 20字节 = 5.72MB
    private final int mappedFileSize;
    // 最大物理偏移量
    private long maxPhysicOffset = -1;
    // 最小逻辑偏移量
    private volatile long minLogicOffset = 0;
    // 消费队列扩展类
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
            final String topic,
            final int queueId,
            final String storePath,
            final int mappedFileSize,
            final DefaultMessageStore defaultMessageStore) {

        // 路径
        this.storePath = storePath;
        // 文件大小
        this.mappedFileSize = mappedFileSize;
        // 默认消息存储实现
        this.defaultMessageStore = defaultMessageStore;

        // 主题、队列
        this.topic = topic;
        this.queueId = queueId;

        // 拼接队列目录
        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;

        // 创建文件队列
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        // 分配 20字节大小 的缓冲区
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        // 是否启用 ConsumeQueue 的扩展功能，默认为 false。
        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                    defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                    defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    /**
     * 加载
     *
     * @return boolean
     */
    public boolean load() {
        // 核心：文件队列加载
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                                + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                                + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                            + (processOffset + mappedFileOffset));
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }


    /**
     * 作用是根据时间戳定位消息在 ConsumeQueue 中的物理偏移量
     *
     * @param timestamp 时间戳
     * @return consumeQueueOffset
     */
    public long getOffsetInQueueByTime(final long timestamp) {

        // 根据传入的时间戳，从 mappedFiles 中获取最后修改时间晚于传入时间戳的 MappedFile 对象
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            // 记录要查找的 消息的offset
            long offset = 0;

            // 首先计算最低查找偏移量，取消息队列最小偏移量与该文件最小偏移量的差为最小偏移量low。
            // 计算低位和高位，用于后续二分查找
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;

            // 获取消息存储中最小的消息物理偏移量（commit log offset）。
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            // 从MappedFile的起始位置开始获取SelectMappedBufferResult对象
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {

                // 缓冲区
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                // 计算high值，即MappedFile中最后一个消息的位置
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    // 二分查找符合要求的消息
                    while (high >= low) {
                        // 计算中间位置偏移量
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        // 将byteBuffer的位置移动到中间位置处
                        byteBuffer.position(midOffset);

                        // 当前条目中的 commitLogOffset
                        long phyOffset = byteBuffer.getLong();
                        // 当前条目中的 消息大小
                        int size = byteBuffer.getInt();

                        // 如果得到的物理偏移量小于当前的最小物理偏移量，说明待查找消息的物理偏移量肯定大于midOffset，则将low设置为midOffset，继续折半查找。
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        // 如果得到的物理偏移量大于最小物理偏移量，说明该消息是有效消息，则根据消息偏移量和消息长度获取消息的存储时间戳。
                        // 获取指定消息在 CommitLog 中的存储时间戳。如果发生错误，则返回 -1。
                        long storeTime =
                                this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);

                        // 如果存储时间小于0，则为无效消息，直接返回0。
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            // 如果存储时间戳等于待查找时间戳，说明查找到了匹配消息，则设置targetOffset并跳出循环。
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            // 如果存储时间戳大于待查找时间戳，说明待查找消息的物理偏移量小于midOffset，则设置high为midOffset，并设置rightIndexValue等于midOffset。
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            // 如果存储时间戳小于待查找时间戳，说明待查找消息的物理偏移量大于midOffset，则设置low为midOffset，并设置leftIndexValue等于midOffset
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    // 如果targetOffset不等于-1，表示找到了存储时间戳等于待查找时间戳的消息
                    if (targetOffset != -1) {
                        // 记录
                        offset = targetOffset;
                    } else {
                        // 如果leftIndexValue等于-1，表示返回当前时间戳大于待查找消息的时间戳，并且最接近待查找消息的偏移量。
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {
                            // 如果rightIndexValue等于-1，表示返回的时间戳比待查找消息的时间戳小，并且最接近待查找消息的偏移量
                            offset = leftOffset;
                        } else {
                            offset =
                                    Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                            - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    // 计算返回 consumeQueueOffset， 即下标索引。
                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    /**
     * 该方法的作用是截断 ConsumeQueue 的逻辑文件，用于清理消费队列中过期的消息，
     *
     * @param phyOffset phyOffset 代表 commitLogOffset 物理偏移量，表示从该 phyOffset 之后的消息不再需要，需要被删除。
     */
    public void truncateDirtyLogicFiles(long phyOffset) {

        int logicFileSize = this.mappedFileSize;
        // 设置最大物理偏移量
        this.maxPhysicOffset = phyOffset;
        long maxExtAddr = 1;

        // 循环读取最后一个MappedFile的ByteBuffer
        while (true) {
            // 获取最后一个 MappedFile 文件
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {

                // 缓冲区
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                // 从头开始遍历 当前MappedFile 文件。 logicFileSize 为文件大小，CQ_STORE_UNIT_SIZE为单个条目的大小。
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {

                    // commitLogOffset、消息大小、tag哈希值
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    // 如果是第一个存储单元
                    if (0 == i) {
                        // 如果消息偏移量大于等于传入的物理偏移量phyOffset，删除最后一个MappedFile，
                        if (offset >= phyOffset) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {

                            // 否则将MappedFile的写入位置、提交位置、刷新位置设置为20（下一个存储单元的开始位置），并更新最大物理偏移量，
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            // 如果tagsCode是扩展地址，则更新maxExtAddr为tagsCode
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        // 检验读取 数据是否合法
                        if (offset >= 0 && size > 0) {

                            // 如果当前读取的 commitLogOffset 大于 指定的phyOffset, 说明这条消息不需要了，所以不需要继续往下执行
                            if (offset >= phyOffset) {
                                return;
                            }

                            // 如果当前读取的 commitLogOffset 小于 指定的phyOffset，说明这条消息需要保留，
                            // 所以需要更新 mappedFile的 写指针、提交指针、刷盘指针。
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);

                            // 更新最大物理偏移量
                            this.maxPhysicOffset = offset + size;


                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            // 读取文件末尾，退出
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }


    /**
     * 获取当前 ConsumeQueue 中最大的 commitLogOffset 偏移量
     *
     * @return 最大的 commitLogOffset 偏移量
     */
    public long getLastOffset() {

        // 记录在当前 ConsumeQueue 中 最大的 commitLogOffset 偏移量
        long lastOffset = -1;

        // 文件大小
        int logicFileSize = this.mappedFileSize;

        // 获取最后一个 MappedFile 文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            // 当前写入指针 - ConsumeQueue条目大小 = 最后一个条目的 读取指针位置 ！！
            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            // 获取 缓冲区
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 设置开始读取位置
            byteBuffer.position(position);

            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {

                // commitLogOffset
                long offset = byteBuffer.getLong();
                // 消息大小
                int size = byteBuffer.getInt();
                // tag哈希值
                byteBuffer.getLong();

                // 如果读取的数据合法 则设置最大的commitLogOffset
                // 不合法，则说明以及去读完毕，则跳出循环
                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }


    /**
     * 刷盘
     *
     * @param flushLeastPages 最少刷盘页数
     * @return boolean
     */
    public boolean flush(final int flushLeastPages) {
        // 持有的文件队列刷盘
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    /**
     * 传入 commitLogOffset ，删除过期的 映射文件
     *
     * @param offset commitLogOffset
     * @return 删除条数
     */
    public int deleteExpiredFile(long offset) {
        // 删除过期的 MappedFile
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        // 校正消费队列的最小物理偏移量
        this.correctMinOffset(offset);
        return cnt;
    }


    /**
     * 校正消费队列的最小物理偏移量
     *
     * @param phyMinOffset 最小物理偏移量
     */
    public void correctMinOffset(long phyMinOffset) {

        // 获取第一个 MappedFile 文件
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // 从MappedFile的起始位置开始获取SelectMappedBufferResult对象
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    // 遍历 MappedFile（消费队列）的所有消息
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        // commitLogOffset
                        long offsetPy = result.getByteBuffer().getLong();
                        // 读取消息大小
                        result.getByteBuffer().getInt();
                        // 读取tag哈希值
                        long tagsCode = result.getByteBuffer().getLong();

                        // 如果当前消息的物理偏移量大于等于 最小物理偏移量，则更新 minLogicOffset，并退出循环
                        if (offsetPy >= phyMinOffset) {
                            // 更新 minLogicOffset
                            this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                    this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            // 如果当前消息有扩展属性，则更新 minExtAddr
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        // 如果启用了扩展读模式，则截断 ConsumeQueueExt
        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }


    /**
     * 获取最小偏移量 （minConsumeQueueOffset）
     *
     * @return minConsumeQueueOffset
     */
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }


    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                            topic, queueId, request.getCommitLogOffset());
                }
            }
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                    request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                        this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }


    /**
     * 将消息在消费队列中的位置信息写入到ConsumeQueue文件中
     *
     * @param offset   commitLogOffset （在 CommitLog 文件的偏移量）
     * @param size     消息大小
     * @param tagsCode 消息tag哈希值
     * @param cqOffset 消息在 ConsumeQueue 中的偏移量。
     * @return boolean
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
                                           final long cqOffset) {

        // 如果 offset+size <= maxPhysicOffset，则认为消息已经处理过，返回 true
        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }

        // 将临时缓冲区的 position 设置为 0，limit 设置为 20（即单个条目的大小）
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);

        // 将消息的物理偏移量、消息大小、tagsCode 写入到临时缓冲区中。
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        // 预期的偏移量
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        // 根据 预期偏移量(计算写入消息在 ConsumeQueue 中的逻辑偏移量) 获取最后一个 MappedFile
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            // 如果是第一个创建的 MappedFile
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {

                // 记录最小偏移量
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                // 填充前置空白区域
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                        + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {

                // 计算当前逻辑偏移量
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                // 检查预期偏移量是否 小于 当前文件的写入位置加上文件的起始偏移量，如果是则返回 true，避免重复写入消息。
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                // 如果预期偏移量与当前逻辑偏移量不相等，则记录错误日志，提示消息队列的顺序可能出错。
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                            "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset,
                            currentLogicOffset,
                            this.topic,
                            this.queueId,
                            expectLogicOffset - currentLogicOffset
                    );
                }
            }

            // 更新最大物理偏移量
            this.maxPhysicOffset = offset + size;
            // 核心：将 消息消费信息 写入到 MappedFile 中。如果写入成功，则返回 true，否则返回 false。
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }


    /**
     * 用于在一个 MappedFile 中填充预留空间的。
     * <p>
     * 这个方法通常在创建一个新的 MappedFile 的时候调用，用于填充上一个 MappedFile 留下的预留空间。
     * 这样可以确保消息的写入顺序，避免因为预留空间的原因导致消息写入的位置出现异常
     *
     * @param mappedFile 映射文件
     * @param untilWhere 预期写入位置
     */
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {

        // 分配 20字节的缓冲区
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        // long 类型 8个字节，对应 commitLogOffset
        byteBuffer.putLong(0L);
        // int 类型 4个字节，对应消息大小
        byteBuffer.putInt(Integer.MAX_VALUE);
        // long 类型 8个字节，对应 tag哈希值
        byteBuffer.putLong(0L);

        // 根据 untilWhere 计算出 MappedFile 中需要填充的字节数 until。
        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        // 以 CQ_STORE_UNIT_SIZE 为步长，循环将 ByteBuffer 中的数据写入到 MappedFile 中，直到填充完毕。
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 根据 startIndex 获取消息消费队列条目
     * <p>
     * 在使用 RocketMQ 消费消息的过程中，消费者需要根据消息的索引信息，从 ConsumeQueue 文件中快速定位消息的物理偏移量和消息大小，以便进行消息消费。
     * 因此，这个方法的使用场景主要是在消息消费阶段
     *
     * @param startIndex ConsumeQueue 逻辑偏移量， 即下标索引。
     * @return SelectMappedBufferResult
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {

        // 文件大小
        int mappedFileSize = this.mappedFileSize;
        // 通过 startIndex × 20 得到在ConsumeQueue文件的物理偏移量，
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        // 如果该偏移量小于 minLogicOffset，则返回null，说明该消息已被删除
        if (offset >= this.getMinLogicOffset()) {

            // 根据偏移量定位到具体的物理文件。
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                // 通过将该偏移量与物理文件的大小取模获取在该文件的偏移量，从偏移量开始连续读取20个字节即可。
                // TODO： selectMappedBuffer(int pos) 是查找 pos到当前最大可读指针之间的数据 ！， 并不是连续读取20个字节 ！！！
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }


    /**
     * 获取消息总数
     *
     * @return ConsumeQueue 队列消息总数
     */
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    /**
     * 获取最大偏移量(maxConsumeQueueOffset)
     *
     * @return maxConsumeQueueOffset
     */
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 自检
     */
    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
                && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
