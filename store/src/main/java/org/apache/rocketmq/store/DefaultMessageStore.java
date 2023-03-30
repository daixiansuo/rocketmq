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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class DefaultMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 消息存储配置
    private final MessageStoreConfig messageStoreConfig;

    // CommitLog 文件的存储实现类
    private final CommitLog commitLog;

    // 消费队列存储缓存表，按消息主题分组
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    // ConsumeQueue 文件刷盘线程，将内存中的 ConsumeQueue 数据刷写到磁盘
    private final FlushConsumeQueueService flushConsumeQueueService;

    // 清除 CommitLog 文件服务
    private final CleanCommitLogService cleanCommitLogService;

    // 清除 ConsumerQueue 文件服务
    private final CleanConsumeQueueService cleanConsumeQueueService;

    // Index 文件实现类
    private final IndexService indexService;

    // MappedFile 分配服务 （内存映射技术）
    private final AllocateMappedFileService allocateMappedFileService;

    // CommitLog 消息分发，根据 CommitLog 文件构建 ConsumeQueue、Index文件。
    private final ReputMessageService reputMessageService;

    // 存储高可用机制
    private final HAService haService;

    // 定时消息服务，用于存储定时消息。
    private final ScheduleMessageService scheduleMessageService;

    // 存储统计服务，用于统计存储信息，例如消息存储量、消费量等。
    private final StoreStatsService storeStatsService;


    /**
     * transientStorePoolEnable机制，即内存级别的读写分离机制
     * <p>
     * 将消息先写入堆外内存并立即返回，然后异步将堆外内存中的数据提交到pagecache，再异步刷盘到磁盘中。
     * 因为堆外内存中的数据并未提交，所以认为是不可信的数据，消息在消费时不会从堆外内存中读取，而是从pagecache中读取，
     * 这样就形成了内存级别的读写分离，即写入消息时主要面对堆外内存，而读取消息时主要面对pagecache。
     * <p>
     * 该机制使消息直接写入堆外内存，然后异步写入pagecache，相比每条消息追加直接写入pagechae，最大的优势是实现了批量化消息写入。
     * <p>
     * 该机制的缺点是如果由于某些意外操作导致broker进程异常退出，已经放入pagecache的数据不会丢失，而存储在堆外内存的数据会丢失。
     */
    private final TransientStorePool transientStorePool;

    // 运行标志，用于标记是否正在运行
    private final RunningFlags runningFlags = new RunningFlags();
    // 系统时钟，用于获取当前睡觉
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));

    // Broker 统计信息管理器，用于记录 Broker 的运行状态信息。
    private final BrokerStatsManager brokerStatsManager;

    // 消息到达监听器，在消息拉取长轮询模式下，当消息到达时会回调该监听器。
    private final MessageArrivingListener messageArrivingListener;

    // Broker 配置属性
    private final BrokerConfig brokerConfig;

    // 是否已经关闭。
    private volatile boolean shutdown = true;

    // 文件刷盘检测点，用于记录存储文件刷盘的位置。
    private StoreCheckpoint storeCheckpoint;

    // 打印次数，用于统计存储信息。
    private AtomicLong printTimes = new AtomicLong(0);

    // CommitLog 文件转发请求队列，用于处理消息分发请求。
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    // 存储锁文件。
    private RandomAccessFile lockFile;

    // 存储锁，用于保证同一时间只有一个进程在运行。
    private FileLock lock;

    boolean shutDownNormal = false;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }
        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService = new HAService(this);
        } else {
            this.haService = null;
        }
        this.reputMessageService = new ReputMessageService();

        this.scheduleMessageService = new ScheduleMessageService(this);

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.allocateMappedFileService.start();

        this.indexService.start();

        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }


    /**
     * 该方法的作用是截断 ConsumeQueue 的逻辑文件，用于清理消费队列中过期的消息，
     *
     * @param phyOffset phyOffset 代表 commitLogOffset 物理偏移量，表示从该 phyOffset 之后的消息不再需要，需要被删除。
     */
    public void truncateDirtyLogicFiles(long phyOffset) {

        // 消费队列存储缓存表
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;
        // 遍历所有 ConsumeQueue 映射文件队列
        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                // 清理消费队列中过期的消息
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * 加载数据
     */
    public boolean load() {
        boolean result = true;

        try {
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            // 延迟消息加载
            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }

            // Commit Log 数据加载
            result = result && this.commitLog.load();

            // Consume Queue 数据加载
            result = result && this.loadConsumeQueue();

            if (result) {

                // 检查点文件
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                // Index 文件加载
                this.indexService.load(lastExitOK);

                // 恢复
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * @throws Exception
     */
    public void start() throws Exception {

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);
        {
            /**
             * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
             * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
             * 3. Calculate the reput offset according to the consume queue;
             * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
             */

            // 计算所有 ConsumeQueue 中的最大 commitLogOffset 偏移量
            long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
            // 遍历所有 ConsumeQueue 队列
            for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
                for (ConsumeQueue logic : maps.values()) {
                    // 最大物理偏移量 是否大于 当前maxPhysicalPosInLogicQueue， 大于则更新maxPhysicalPosInLogicQueue。
                    if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
                        maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
                    }
                }
            }

            // 小于 0 ，则设置为 0。
            if (maxPhysicalPosInLogicQueue < 0) {
                maxPhysicalPosInLogicQueue = 0;
            }

            // 如果 maxPhysicalPosInLogicQueue 小于当前 CommitLog 文件组中最小的偏移量。
            // 则将 maxPhysicalPosInLogicQueue 设置为 当前 CommitLog 文件组中最小的偏移量。
            if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
                maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
                /**
                 * This happens in following conditions:
                 * 1. If someone removes all the consumequeue files or the disk get damaged.
                 * 2. Launch a new broker, and copy the commitlog from other brokers.
                 *
                 * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
                 * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
                 */
                log.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
            }
            log.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
                    maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());


            // maxPhysicalPosInLogicQueue 参数的含义 是ReputMessageService从哪个物理偏移量开始转发消息给ConsumeQueue和Index文件
            this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);
            // 启动消息分发线程
            this.reputMessageService.start();

            /**
             *  1. Finish dispatching the messages fall behind, then to start other services.
             *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
             */
            while (true) {
                if (dispatchBehindBytes() <= 0) {
                    break;
                }
                Thread.sleep(1000);
                log.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
            }
            this.recoverTopicQueueTable();
        }

        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService.start();
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }

        // 启动 ConsumeQueue 文件刷盘线程，将内存中的 ConsumeQueue 数据刷写到磁盘
        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        this.createTempFile();
        this.addScheduleTask();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }
            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }


    /**
     * 将消息存储到存储中。
     *
     * @param msg Message instance to store
     * @return PutMessageResult
     */
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {

        // 如果当前 broker 停止工作，则拒绝写入。
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // 如果当前 broker 是 slave 节点，则拒绝写入。
        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        // 如果当前状态是 不可写状态，则拒绝写入。
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        // 消息主题长度超过 127 个字符， 不合法，拒绝写入。
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 如果消息属性长度超过 32767 个字符，不合法，拒绝写入。
        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        // 如果操作系统页面缓存繁忙 (即是否有过多的I/O请求排队等待缓存或释放页面)，则拒绝写入。
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        // 开始时间
        long beginTime = this.getSystemClock().now();
        // 核心：commitLog 文件写入 ！！！
        PutMessageResult result = this.commitLog.putMessage(msg);

        // 写入耗时
        long elapsedTime = this.getSystemClock().now() - beginTime;
        // 超过 500ms，打印warn日志
        if (elapsedTime > 500) {
            log.warn("putMessage not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
        }

        // 写入完成后，会统计写入所耗费的时间，并更新最大耗时时间
        this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

        // 如果写入失败，则将写入失败的次数加1。
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        if (this.shutdown) {
            log.warn("DefaultMessageStore has shutdown, so putMessages is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("DefaultMessageStore is in slave mode, so putMessages is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("DefaultMessageStore is not writable, so putMessages is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("PutMessages topic length too long " + messageExtBatch.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessages(messageExtBatch);

        long elapsedTime = this.getSystemClock().now() - beginTime;
        if (elapsedTime > 500) {
            log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
        }
        this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    @Override
    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        return diff < 10000000
                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
                                       final int maxMsgNums,
                                       final MessageFilter messageFilter) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            maxPhyOffsetPulling = offsetPy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                    isInDisk)) {
                                break;
                            }

                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                            tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            if (messageFilter != null
                                    && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            if (messageFilter != null
                                    && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                            getResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        if (diskFallRecorded) {
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }

                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                                * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long elapsedTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return getStoreTime(result);
        }

        return -1;
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                            cq.getTopic(),
                            cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                                nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId(),
                                nextQT.getValue().getMaxPhysicOffset(),
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                                topic,
                                nextQT.getKey(),
                                minCommitLogOffset,
                                maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
                                           SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                    this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (maxMsgNums <= messageTotal) {
            return true;
        }

        if (isInDisk) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * 创建 abort 文件，用于判断是否正常退出
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    /**
     * 添加定时任务
     */
    private void addScheduleTask() {

        // 清理 CommitLog、ConsumeQueue 过期文件，资源回收间隔，默认为 10000ms，即10s执行一次。
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        // 定期自检，启动后一分钟执行一次，然后每隔10分钟执行一次。
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);


        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // 是否启用debug锁，默认为false。启用该功能后，会在debug模式下打印锁调用栈信息，方便锁调试。设计思想是为了方便调试和排查锁问题。
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                        + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }

    /**
     * 定期清理过期文件
     */
    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }


    /**
     * 加载 ConsumeQueue 文件
     *
     * @return boolean
     */
    private boolean loadConsumeQueue() {

        // 打开 consumeQueue 存储目录
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        // 获取目录下所有主题目录
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            // 遍历所有主题
            for (File fileTopic : fileTopicList) {

                // 文件名称 = 主题名称
                String topic = fileTopic.getName();
                // 获取目录下 所有队列目录
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    // 遍历队列所有队列
                    for (File fileQueueId : fileQueueIdList) {

                        int queueId;
                        try {
                            // 队列ID
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }

                        // 创建逻辑 消费队列
                        ConsumeQueue logic = new ConsumeQueue(
                                topic,
                                queueId,
                                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                                this);
                        // 添加到 消息队列存储缓存表
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    private void recover(final boolean lastExitOK) {
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();

        if (lastExitOK) {
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }

        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    /**
     * 添加到 消息队列存储缓存表
     *
     * @param topic        主题
     * @param queueId      队列ID
     * @param consumeQueue 消费队列
     */
    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        // 根据topic查询
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            // 不存在创建、添加
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            // 覆盖添加
            map.put(queueId, consumeQueue);
        }
    }

    private long recoverConsumeQueue() {
        long maxPhysicOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }

        return maxPhysicOffset;
    }

    public void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQueue());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }


    /**
     * 用于同步 commitLog 消息数据到 ConsumeQueue、IndexFile 中！
     *
     * @param req 分发请求
     */
    public void doDispatch(DispatchRequest req) {
        // 遍历分发器列表
        // CommitLogDispatcherBuildConsumeQueue、CommitLogDispatcherBuildConsumeQueue
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }


    /**
     * 调度构建 ConsumeQueue 消息，即向 ConsumeQueue 文件中写入数据。
     *
     * @param dispatchRequest 调度请求
     */
    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        // 根据 topic、queueId 查询对应 ConsumeQueue
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        // 写入消息
        cq.putMessagePositionInfoWrapper(dispatchRequest);
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    @Override
    public void handleScheduleMessageService(final BrokerRole brokerRole) {
        if (this.scheduleMessageService != null) {
            if (brokerRole == BrokerRole.SLAVE) {
                this.scheduleMessageService.shutdown();
            } else {
                this.scheduleMessageService.start();
            }
        }

    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.availableBufferNums();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }


    // ==================================== 内部类 ===========================================


    /**
     * CommitLog 文件转发，用于处理消息分发至 ConsumeQueue 中。
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {

            // 获取消息类型，不同消息类型处理策略不同
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // 非事物消息、事物提交消息 才进行分发同步 ！！
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    /**
     * CommitLog 文件转发，用于处理消息分发至 Index 中。
     */
    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            // 消息索引是否开启，默认为开启
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                // 根据 commitLog 消息 构建 index 文件数据。
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }


    /**
     * 清除 CommitLog 过期文件服务
     */
    class CleanCommitLogService {

        // 手动删除最大次数
        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;

        /**
         * 通过系统参数Drocketmq.broker.diskSpaceWarningLevelRatio进行设置，默认0.90。
         * 如果磁盘分区使用率超过该阈值，将设置磁盘为不可写，此时会拒绝写入新消息
         */
        private final double diskSpaceWarningLevelRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        /**
         * 通过系统参数Drocketmq.broker.diskSpaceCleanForcibly-Ratio进行设置，默认0.85。
         * <p>
         * 如果磁盘分区使用超过该阈值，建议立即执行过期文件删除，但不会拒绝写入新消息
         */
        private final double diskSpaceCleanForciblyRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));

        // 记录上次 删除挂起文件 的时间
        private long lastRedeleteTimestamp = 0;

        // 手动触发
        private volatile int manualDeleteFileSeveralTimes = 0;

        // 是否立即清理
        private volatile boolean cleanImmediately = false;

        /**
         * 手动执行删除过期文件
         * TODO ？？
         */
        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        /**
         * 上层定时调用
         * invoke: org.apache.rocketmq.store.DefaultMessageStore#cleanFilesPeriodically()
         */
        public void run() {
            try {
                // 删除过期文件
                this.deleteExpiredFiles();
                // 定期检查并删除挂起文件
                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * 删除CommitLog过期文件
         */
        private void deleteExpiredFiles() {

            // 删除数量
            int deleteCount = 0;
            // CommitLog 日志文件保留时间，默认为 72 小时。
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            // CommitLog 文件删除间隔，默认为 100ms
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            // 强制销毁 MapedFile 的时间间隔，默认为 1000 * 120ms
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            // 是否到删除过期文件的时间，默认是每天凌晨4点
            boolean timeup = this.isTimeToDelete();
            // 检查磁盘空间是否充足，如果磁盘空间不充足，则返回true，表示应该触发过期文件删除操作。
            boolean spacefull = this.isSpaceToDelete();
            // 预留手工触发机制，可以通过调用excuteDeleteFilesManualy方法手工触发删除过期文件的操作，目前RocketMQ暂未封装手工触发文件删除的命令
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;


            // 三者满足任何一个条件 就执行删除过期文件
            if (timeup || spacefull || manualDelete) {

                // 手动删除次数递减
                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                // 是否立即清理文件，若为true，则无论是否过期都会立即删除。
                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                        fileReservedTime,
                        timeup,
                        spacefull,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce);

                // 重要：小时 转 毫秒 ！！！
                fileReservedTime *= 60 * 60 * 1000;

                // 删除过期的文件
                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                        destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }


        /**
         * 定期检查并删除挂起文件
         */
        private void redeleteHangedFile() {

            // 定期检查并删除挂起文件的时间间隔，默认为 1000 * 120ms
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            // 检查是否达到删除周期
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                // 更新记录删除时间
                this.lastRedeleteTimestamp = currentTimestamp;
                //  强制销毁 MapedFile 的时间间隔，默认为 1000 * 120ms。
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                // 重试删除存储路径下的第一个MappedFile文件 （只有当第一个 MappedFile 不可用时才删除！）
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        /**
         * 是否到删除过期文件的时间，默认配置，每天4点删除过期文件。
         *
         * @return boolean
         */
        private boolean isTimeToDelete() {
            // 每天几点删除过期文件，默认为 04 点。
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        /**
         * 判断磁盘空间是否充足
         *
         * @return boolean
         */
        private boolean isSpaceToDelete() {

            // 磁盘最大使用空间比例，默认为 75%。
            // 表示CommitLog文件、ConsumeQueue文件所在磁盘分区的最大使用量，如果超过该值，则需要立即清除过期文件。
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            // 表示是否需要立即执行清除过期文件的操作。
            cleanImmediately = false;

            {
                // 存储 commitLog 文件路径
                String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                /*
                    当前CommitLog目录所在的磁盘分区的磁盘使用率。
                    通过File#getTotal Space方法获取文件所在磁盘分区的总容量，通过File#getFreeSpace方法获取文件所在磁盘分区的剩余容量。
                 */
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

                /*
                    通过系统参数Drocketmq.broker.diskSpaceWarningLevelRatio进行设置，默认0.90。
                    TODO：如果磁盘分区使用率超过该阈值，将设置磁盘为不可写，此时会拒绝写入新消息
                 */
                if (physicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    /*
                        通过系统参数Drocketmq.broker.diskSpaceCleanForcibly-Ratio进行设置，默认0.85。
                        如果磁盘分区使用超过该阈值，建议立即执行过期文件删除，但不会拒绝写入新消息
                     */
                    cleanImmediately = true;
                } else {
                    // 如果磁盘使用率低于diskSpaceCleanForciblyRatio将恢复磁盘可写
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                // 如果当前磁盘使用率小于diskMaxUsedSpaceRatio，则返回false，表示磁盘使用率正常，否则返回true，需要执行删除过期文件
                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            {
                // 存储 consumeQueue 文件路径
                String storePathLogics = StorePathConfigHelper
                        .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
                // 当前 ConsumeQueue 目录所在的磁盘分区的磁盘使用率。
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);

                // 同上
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }


    /**
     * 清除 ConsumerQueue 过期文件服务
     */
    class CleanConsumeQueueService {

        // 最后一次处理的 最小物理偏移量（commitLogOffset）
        private long lastPhysicalMinOffset = 0;

        /**
         * 上层定时调用
         * invoke: org.apache.rocketmq.store.DefaultMessageStore#cleanFilesPeriodically()
         */
        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * 删除过期文件
         */
        private void deleteExpiredFiles() {

            //  ConsumeQueue 文件删除间隔，默认为 100ms
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();
            // 获取 commitLog 最小物理偏移量
            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();

            // 当前的最小物理偏移量 是否大于 上一次处理记录的最小物理偏移量
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                // 消费队列存储缓存表
                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;
                // 遍历所有 ConsumeQueue
                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {

                        // 传入当前 commitLog 的最小物理偏移量，删除小于此偏移量的 所有MappedFile文件！
                        int deleteCount = logic.deleteExpiredFile(minOffset);
                        // 删除文件数量 & 文件删除间隔
                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                // 同时，根据 commitLog 最小物理偏移量，删除 index 文件中的小于此偏移量的 MappedFile 文件！
                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    /**
     * ConsumeQueue 文件刷盘线程
     */
    class FlushConsumeQueueService extends ServiceThread {

        // 重试次数
        private static final int RETRY_TIMES_OVER = 3;
        // 记录最后刷盘时间
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {

            // 刷盘ConsumeQueue时的最小刷盘页数，默认为2。
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            // 如果当前是重试，则将最小刷盘页数设为0。
            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;
            // 消费队列刷盘时间间隔，默认值为1分钟（1000 * 60毫秒）。
            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();

            // 如果当前时间距离上次刷盘超过了设定的阈值，则将最小刷盘页数设为0，并获取逻辑消息时间戳
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                // 从 检查点文件 获取上次更新时间
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            // 所有 ConsumeQueue 文件
            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;
            // 对每个队列执行刷盘操作
            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        // 刷盘
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            // 如果最小刷盘页数为0，则将逻辑消息时间戳刷盘，同时刷盘存储检查点。
            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // ConsumeQueue 刷盘间隔，默认为 1000ms
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    // 执行 doFlush(1) 方法，将 ConsumeQueue 刷写到磁盘；
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 如果 FlushConsumeQueueService 被停止，则执行 doFlush(RETRY_TIMES_OVER) 方法，将 ConsumeQueue 刷写到磁盘，并结束该线程；
            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }


    /**
     * Broker服务器在启动时会启动ReputMessageService线程，并初始化一个非常关键的参数reputFromOffset，
     * 该参数的含义是ReputMessageService从哪个物理偏移量开始转发消息给ConsumeQueue和Index文件。
     * <p>
     * 如果允许重复转发，将reputFromOffset设置为CommitLog文件的提交指针。如果不允许重复转发，将reputFromOffset设置为CommitLog文件的内存中最大偏移量，
     */
    class ReputMessageService extends ServiceThread {

        // reputFromOffset 参数的含义 是ReputMessageService从哪个物理偏移量开始转发消息给ConsumeQueue和Index文件
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        /**
         * 计算目前 还落后 commitLog 多少数据
         *
         * @return 落后数据数量
         */
        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        /**
         * 判断是否还有数据是否未 同步转发
         *
         * @return boolean
         */
        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        /**
         * 消息转发
         */
        private void doReput() {

            // 如果消息当前 commitLog 文件中最小偏移量，那就从 最小偏移量开始转发！
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                log.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                        this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }


            // 一直到 commitLog 没有数据需要同步转发，才退出循环
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                // 是否启用消息去重功能，默认为false。
                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                        && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 根据 reputFromOffset 查询对应的MappedFile文件，然后读取从 reputFromOffset 到对应MappedFile 中最大可读指针之间的数据 ！
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        // 更新同步位置（偏移量）
                        this.reputFromOffset = result.getStartOffset();

                        // 遍历查询到的所有数据，当没有数据可遍历时 跳出循环
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                            // 从result返回的ByteBuffer中循环读取消息，一次读取一条，创建Dispatch Request对象。
                            DispatchRequest dispatchRequest =
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            // 如果 bufferSize = -1，则获取消息大小。
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            // 成功读取一条消息
                            if (dispatchRequest.isSuccess()) {
                                // 消息长度大于0，则调用doDispatch()方法
                                if (size > 0) {

                                    // TODO 核心: 调用分发 !!!
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                                    // 如果当前是 master节点，且开启消息拉取长轮询模式，当消息到达时会回调该监听器。
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                        // 回调该监听器
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                    }

                                    // 更新同步位置，加上已经分发过的这条消息的长度
                                    this.reputFromOffset += size;
                                    // 更新读取大小，用于跳出循环
                                    readSize += size;

                                    // 如果当前节点是 slave节点，统计当前topic下消息数量、以及 topic下的 消息数据总大小。
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).incrementAndGet();
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                                .addAndGet(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) {
                                    // 表明数据已经读取完毕
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    // 设置跳出循环条件
                                    readSize = result.getSize();
                                }
                            } else if (!dispatchRequest.isSuccess()) {

                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    // If user open the dledger pattern or the broker is master node,
                                    // it will not ignore the exception and fix the reputFromOffset variable
                                    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog() ||
                                            DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                this.reputFromOffset);
                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        // 释放缓冲区
                        result.release();
                    }
                } else {
                    // 当前没有数据需要同步时，跳出循环
                    doNext = false;
                }
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            // ReputMessageService线程每执行一次任务推送，休息1ms后继续尝试推送消息到ConsumeQueue和Index文件中
            while (!this.isStopped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
