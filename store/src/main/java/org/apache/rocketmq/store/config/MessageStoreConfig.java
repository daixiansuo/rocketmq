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
package org.apache.rocketmq.store.config;

import java.io.File;

import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.store.ConsumeQueue;

/**
 * 消息存储配置
 */
public class MessageStoreConfig {


    // 存储日志数据的根目录，该属性为必要属性，如果未设置，将使用用户主目录下的 store 目录作为存储路径。
    // The root directory in which the log data is kept
    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    // CommitLog 文件所在的目录，也是必要属性，如果未设置，则使用 storePathRootDir + "/commitlog" 作为存储路径。
    //The directory in which the commitlog is kept
    @ImportantField
    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "commitlog";


    // CommitLog 文件的大小，默认为 1G，即 1024 * 1024 * 1024 字节。设计思想是为了限制单个 CommitLog 文件的大小，防止文件过大造成的性能问题。
    // CommitLog file size,default is 1G
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    /**
     * ConsumeQueue 文件的大小，默认为 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE，其中 CQ_STORE_UNIT_SIZE 为 ConsumeQueue 中存储单元的大小，默认为 20 字节。
     * 每个ConsumeQueue文件大小约5.72MB。
     * 设计思想是为了限制单个 ConsumeQueue 文件的大小，防止文件过大造成的性能问题。
     */
    // ConsumeQueue file size,default is 30W
    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;


    // 是否启用 ConsumeQueue 的扩展功能，默认为 false。
    // enable consume queue ext
    private boolean enableConsumeQueueExt = false;

    // ConsumeQueue 扩展文件的大小，默认为 48M。
    // ConsumeQueue extend file size, 48M
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;

    // ConsumeQueue 扩展文件的位图长度，默认为 64。
    // Bit count of filter bit map.
    // this will be set by pipe of calculate filter bit map.
    private int bitMapLengthConsumeQueueExt = 64;


    // CommitLog 刷新磁盘的间隔时间，默认为 500ms
    // CommitLog flush interval, flush data to disk。
    @ImportantField
    private int flushIntervalCommitLog = 500;

    // TODO：当启用 TransientStorePool 功能时，CommitLog 刷新磁盘的间隔时间，默认为 200ms
    // flush data to FileChannel （Only used if TransientStorePool enabled）
    @ImportantField
    private int commitIntervalCommitLog = 200;

    /**
     * 在将消息放入队列时，是否使用互斥锁。默认为 false，表示使用自旋锁。
     * <p>
     * 设计思想：使用互斥锁会带来一定的性能开销，但可以防止多个线程同时写入消息时发生竞争和冲突的问题，确保数据的一致性和可靠性。
     * <p>
     * introduced since 4.0.x. Determine whether to use mutex reentrantLock when putting message.<br/>
     * By default it is set to false indicating using spin lock when putting message.
     */
    private boolean useReentrantLockWhenPutMessage = false;

    // 是否定时刷新 CommitLog，默认为实时刷新。
    // Whether schedule flush,default is real-time
    @ImportantField
    private boolean flushCommitLogTimed = false;

    // ConsumeQueue 刷盘间隔，默认为 1000ms
    // ConsumeQueue flush interval
    private int flushIntervalConsumeQueue = 1000;

    // 资源回收间隔，默认为 10000ms。设计思想是为了定期回收不再使用的资源，避免占用过多内存或磁盘空间，影响系统稳定性
    // Resource reclaim interval
    private int cleanResourceInterval = 10000;

    // CommitLog 文件删除间隔，默认为 100ms。设计思想是为了定期删除已经被消费完毕的 CommitLog 文件，避免占用过多磁盘空间，提高系统可用性。
    // CommitLog removal interval
    private int deleteCommitLogFilesInterval = 100;

    // ConsumeQueue 文件删除间隔，默认为 100ms。同上。
    // ConsumeQueue removal interval
    private int deleteConsumeQueueFilesInterval = 100;

    // 强制销毁 MapedFile 的时间间隔，默认为 1000 * 120ms。设计思想是为了定期清理无效的 MapedFile 文件，释放磁盘空间。
    private int destroyMapedFileIntervalForcibly = 1000 * 120;

    // 定期检查并删除挂起文件的时间间隔，默认为 1000 * 120ms。设计思想是为了避免因程序崩溃或非正常停止等原因导致的未处理异常情况，定期检查并删除挂起文件，保证系统稳定性。
    private int redeleteHangedFileInterval = 1000 * 120;

    // 每天几点删除过期文件，默认为 04 点。设计思想是为了定期清理过期的文件，避免占用过多磁盘空间，提高系统可用性。
    // When to delete,default is at 4 am
    @ImportantField
    private String deleteWhen = "04";

    // 磁盘最大使用空间比例，默认为 75%。设计思想是为了防止磁盘空间被占用过多，导致系统出现问题。
    private int diskMaxUsedSpaceRatio = 75;

    // 日志文件保留时间，默认为 72 小时。设计思想是为了定期清理过期的日志文件，避免占用过多磁盘空间，提高系统可用性。
    // The number of hours to keep a log file before deleting it (in hours)
    @ImportantField
    private int fileReservedTime = 72;

    // ConsumeQueue 流控阈值，默认为 600000。设计思想是为了限制消息写入的速度，防止生产者发送消息速度过快
    // Flow control for ConsumeQueue
    private int putMsgIndexHightWater = 600000;


    /**
     * 消息的最大大小，默认为4M。当一个消息的大小超过该阈值时，Broker将拒绝该消息并返回错误。
     * 该属性的设计思想是为了限制消息的大小，避免因单个消息过大导致Broker出现性能问题或内存溢出
     * <p>
     * The maximum size of message,default is 4M
     */
    private int maxMessageSize = 1024 * 1024 * 4;


    /**
     * 恢复消息时是否校验消息的CRC32校验码。该属性默认开启，用于确保消息在传输或存储过程中没有损坏。
     * 由于校验过程需要计算CRC32校验码，因此在性能要求极高的场景下可以将其关闭。该属性的设计思想是为了提高消息传输和存储的可靠性。
     * <p>
     * Whether check the CRC32 of the records consumed.
     * This ensures no on-the-wire or on-disk corruption to the messages occurred.
     * This check adds some overhead,so it may be disabled in cases seeking extreme performance.
     */
    private boolean checkCRCOnRecover = true;


    /**
     * 刷盘CommitLog时的最小刷盘页数，默认为4。CommitLog是消息持久化的主要存储，该属性用于控制刷盘的频率和效率。
     * 在高吞吐量场景下，频繁刷盘会对性能造成较大的影响，因此设置一个较小的最小刷盘页数可以减少刷盘次数，提高性能。
     * <p>
     * How many pages are to be flushed when flush CommitLog
     */
    private int flushCommitLogLeastPages = 4;

    // 提交数据到文件时的最小提交页数，默认为4。该属性控制将数据写入磁盘的频率，较小的提交页数可以减少磁盘写入的次数，提高性能。
    // How many pages are to be committed when commit data to file
    private int commitCommitLogLeastPages = 4;

    /**
     * 磁盘处于预热状态时刷盘的页数，默认为1024/4*16=4096。
     * 预热状态指的是磁盘的写入速度远低于正常状态，例如磁盘正在进行磁盘清理、磁盘碎片整理等操作时。
     * 该属性的设计思想是为了避免在预热状态下频繁刷盘导致性能问题。
     * <p>
     * Flush page size when the disk in warming state
     */
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;

    /**
     * 刷盘ConsumeQueue时的最小刷盘页数，默认为2。ConsumeQueue是消费队列，用于保存消息消费进度，该属性用于控制ConsumeQueue的刷盘频率和效率。
     * <p>
     * How many pages are to be flushed when flush ConsumeQueue
     */
    private int flushConsumeQueueLeastPages = 2;

    /**
     * 刷盘CommitLog时的强制刷盘间隔，默认为10000ms（10秒）。在该时间内如果没有进行CommitLog的刷盘操作，则会强制刷盘。
     * 该属性的设计思想是为了确保消息的可靠性，在一定时间内将消息从内存中刷盘到磁盘上，防止因Broker宕机或其他原因导致消息丢失。
     */
    private int flushCommitLogThoroughInterval = 1000 * 10;

    /**
     * 将数据提交到文件时的强制提交间隔，默认为200ms。在该时间内如果没有提交数据，则会强制提交。
     */
    private int commitCommitLogThoroughInterval = 200;

    /**
     * 消费队列刷盘时间间隔，默认值为1分钟（1000 * 60毫秒）。
     * 该变量用于指定消费队列刷盘的时间间隔，消费队列中的消息在被消费后需要被刷盘以确保消息被持久化，避免数据丢失。
     * 设置该时间间隔可以控制刷盘的频率，以免过于频繁导致性能下降。
     */
    private int flushConsumeQueueThoroughInterval = 1000 * 60;

    /**
     * 内存中消息存储时的单条消息最大字节数，默认值为256KB（1024 * 256）。
     * 该变量用于限制内存中存储的单条消息的大小，当单条消息超过该大小时，会被转移到磁盘中进行存储。设置该变量可以控制内存中存储消息的大小，避免占用过多的内存空间。
     */
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;

    /**
     * 内存中消息存储时的最大消息条数，默认值为32。
     * 该变量用于限制内存中存储的消息条数，当消息条数超过该值时，会有一部分消息被转移到磁盘中进行存储。设置该变量可以控制内存中存储消息的数量，避免占用过多的内存空间。
     */
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;

    /**
     * 磁盘中消息存储时的单条消息最大字节数，默认值为64KB（1024 * 64）。
     * 该变量用于限制磁盘中存储的单条消息的大小，当单条消息超过该大小时，会被拆分成多个文件进行存储。设置该变量可以控制磁盘中存储消息的大小，避免占用过多的磁盘空间。
     */
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;

    /**
     * 磁盘中消息存储时的最大消息条数，默认值为8。
     * 该变量用于限制磁盘中存储的消息条数，当消息条数超过该值时，会有一部分消息被转移出当前文件进行存储。设置该变量可以控制磁盘中存储消息的数量，避免占用过多的磁盘空间。
     */
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;

    /**
     * accessMessageInMemoryMaxRatio 字段表示内存中最大存储消息的比例，默认值为40。
     * 在消息存储过程中，RocketMQ会将消息先存储在内存中，当内存中的消息数量达到一定比例后，会将部分消息转移到磁盘中进行持久化存储。
     * accessMessageInMemoryMaxRatio 字段的值就是指定了内存中存储消息的比例，当内存中的消息数量达到这个比例时，就会触发消息的持久化操作
     */
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;

    /**
     * 消息索引是否开启，默认为开启。该变量用于指定是否开启消息索引功能，开启后可以提高
     */
    @ImportantField
    private boolean messageIndexEnable = true;


    // 表示消息索引哈希槽数量的最大值。设计思想是通过哈希算法快速定位消息所在文件以提高消息读取速度。
    private int maxHashSlotNum = 5000000;
    // 表示消息索引总数的最大值，是消息存储系统中消息索引的数量上限。设计思想是通过限制索引总数，以保证消息存储系统的稳定性和高效性。
    private int maxIndexNum = 5000000 * 4;
    // 表示批量获取消息时的最大消息数。设计思想是在保证消息读取效率的前提下，限制单次读取消息的数量，以防止因过多消息读取而导致存储系统性能下降。
    private int maxMsgsNumBatch = 64;

    /**
     * 是否开启消息索引安全检查。
     * 当该值为 true 时，消息存储模块会在写入消息索引时进行校验，确保写入的消息索引没有被篡改。设计思想是保障消息存储的数据安全。
     */
    @ImportantField
    private boolean messageIndexSafe = false;
    // 表示高可用模式下从节点监听的端口号。设计思想是通过该端口号实现从节点和主节点的通信。
    private int haListenPort = 10912;

    /**
     * 高可用模式下从节点向主节点发送心跳的间隔时间。
     * 该值用于配置从节点向主节点发送心跳的间隔时间，以便主节点检测从节点的状态。设计思想是通过心跳机制实现主从节点之间的状态监测。
     */
    private int haSendHeartbeatInterval = 1000 * 5;

    /**
     * 高可用模式下从节点的清理间隔时间。
     * 该值用于配置从节点对过期的数据进行清理的时间间隔。设计思想是通过定时清理机制避免从节点存储空间的浪费。
     */
    private int haHousekeepingInterval = 1000 * 20;

    /**
     * 高可用模式下主节点向从节点传输数据的批量大小。
     * 该值用于配置主节点向从节点传输数据的批量大小，以便控制数据传输的效率和网络带宽的占用。设计思想是在保证数据传输效率的同时，避免网络资源的浪费。
     */
    private int haTransferBatchSize = 1024 * 32;


    /**
     * 高可用模式下主节点的地址。
     * 该值用于配置从节点连接的主节点地址，以便从节点向主节点请求数据。设计思想是通过网络连接实现主从节点之间的数据同步。
     */
    @ImportantField
    private String haMasterAddress = null;

    // 高可用模式下，从节点最大允许与主节点数据落后的字节数。超过这个值，从节点将被视为失效，由其他从节点接替。
    private int haSlaveFallbehindMax = 1024 * 1024 * 256;

    /**
     * 消息服务的角色，包括异步复制主节点（ASYNC_MASTER）和同步复制主节点（SYNC_MASTER）。
     * 异步复制主节点会将写入请求异步地复制到从节点，而同步复制主节点会等待从节点返回确认后再响应写入请求。
     */
    @ImportantField
    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;

    /**
     * 磁盘刷盘类型，包括异步刷盘（ASYNC_FLUSH）和同步刷盘（SYNC_FLUSH）。
     * 异步刷盘可以提高写入性能，但可能会丢失部分数据；同步刷盘可以保证数据不丢失，但会降低写入性能。
     */
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;

    // 同步刷盘超时时间，单位为毫秒。当消息存储方式为同步刷盘模式时，如果刷盘时间超过该值，则认为刷盘失败。
    private int syncFlushTimeout = 1000 * 5;
    // 消息延迟级别列表，用于设置消息的延迟发送时间。
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    // 刷盘延迟偏移量，表示消息在存储时的延迟时间。例如，若值为 10s，那么 RocketMQ 会在实际需要刷盘前的 10 秒进行预刷盘，提高磁盘写入效率。
    private long flushDelayOffsetInterval = 1000 * 10;

    /**
     * 是否启用强制删除过期文件功能。
     * 如果启用，当存储目录超过一定大小限制时，RocketMQ 会自动删除过期文件。
     */
    @ImportantField
    private boolean cleanFileForciblyEnable = true;


    /**
     * 是否启用预热MappedFile，默认为false。
     * MappedFile是RocketMQ中用于存储消息的文件，启用预热功能可以在创建MappedFile时提前将数据从内存写入磁盘，
     * 加快消息的写入速度。设计思想是通过提前将数据写入磁盘，降低写入时的IO负载，提升写入性能。
     */
    private boolean warmMapedFileEnable = false;
    /**
     * 是否在slave节点中检查commitlog偏移量，默认为false。
     * 当主从同步中断并重新连接时，可能会发生主从偏移量不一致的情况，启用该功能可以检测出偏移量不一致的情况并进行修复。设计思想是确保主从同步的数据一致性。
     */
    private boolean offsetCheckInSlave = false;

    // 是否启用debug锁，默认为false。启用该功能后，会在debug模式下打印锁调用栈信息，方便锁调试。设计思想是为了方便调试和排查锁问题。
    private boolean debugLockEnable = false;

    /**
     * 是否启用消息去重功能，默认为false。
     * 启用该功能后，可以对发送的消息进行去重，确保消息不会重复消费。设计思想是确保消息的幂等性，避免重复消费。
     */
    private boolean duplicationEnable = false;

    /**
     * 是否记录磁盘错误日志，默认为true。
     * 启用该功能可以记录磁盘错误信息，方便排查磁盘故障。设计思想是为了方便排查磁盘问题。
     */
    private boolean diskFallRecorded = true;

    /**
     * 系统页缓存忙时超时时间，默认为1000毫秒。
     * 当系统页缓存繁忙时，会使用同步写磁盘的方式写入数据，设置该参数可以设置超时时间，如果超时则使用异步写磁盘的方式写入数据。
     * 设计思想是为了提高写入性能，避免系统页缓存繁忙时的写入阻塞。
     */
    private long osPageCacheBusyTimeOutMills = 1000;

    /**
     * 消息查询时默认最大返回条数，默认为32条。
     * 设计思想是为了限制消息查询的返回结果数量，避免一次查询返回过多数据造成的性能问题
     */
    private int defaultQueryMaxNum = 32;

    /**
     * 是否启用消息暂存池功能。
     * 启用后，消息将在写入时暂存于内存中，然后批量刷盘到磁盘，提高磁盘写入效率。
     */
    @ImportantField
    private boolean transientStorePoolEnable = false;
    /**
     * 消息暂存池大小，单位为 MB。
     * 消息写入时暂存于内存中，当消息暂存池占满后，会触发批量刷盘操作。
     */
    private int transientStorePoolSize = 5;
    /**
     * 在消息暂存池可用空间不足时，是否快速失败。
     * 如果该值为 true，则在消息暂存池可用空间不足时，消息将直接返回发送失败；否则，消息将等待暂存池可用空间，然后再进行发送。
     */
    private boolean fastFailIfNoBufferInStorePool = false;

    // 是否启用分布式日志存储。启用后，消息将会存储到分布式存储系统中，提高可靠性和数据安全性。
    private boolean enableDLegerCommitLog = false;
    // 分布式日志存储组名
    private String dLegerGroup;
    // 分布式日志存储节点地址列表，多个地址以逗号分隔。
    private String dLegerPeers;
    // 分布式日志存储节点 ID。
    private String dLegerSelfId;

    public boolean isDebugLockEnable() {
        return debugLockEnable;
    }

    public void setDebugLockEnable(final boolean debugLockEnable) {
        this.debugLockEnable = debugLockEnable;
    }

    public boolean isDuplicationEnable() {
        return duplicationEnable;
    }

    public void setDuplicationEnable(final boolean duplicationEnable) {
        this.duplicationEnable = duplicationEnable;
    }

    public long getOsPageCacheBusyTimeOutMills() {
        return osPageCacheBusyTimeOutMills;
    }

    public void setOsPageCacheBusyTimeOutMills(final long osPageCacheBusyTimeOutMills) {
        this.osPageCacheBusyTimeOutMills = osPageCacheBusyTimeOutMills;
    }

    public boolean isDiskFallRecorded() {
        return diskFallRecorded;
    }

    public void setDiskFallRecorded(final boolean diskFallRecorded) {
        this.diskFallRecorded = diskFallRecorded;
    }

    public boolean isWarmMapedFileEnable() {
        return warmMapedFileEnable;
    }

    public void setWarmMapedFileEnable(boolean warmMapedFileEnable) {
        this.warmMapedFileEnable = warmMapedFileEnable;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public void setMappedFileSizeCommitLog(int mappedFileSizeCommitLog) {
        this.mappedFileSizeCommitLog = mappedFileSizeCommitLog;
    }

    public int getMappedFileSizeConsumeQueue() {

        int factor = (int) Math.ceil(this.mappedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        return (int) (factor * ConsumeQueue.CQ_STORE_UNIT_SIZE);
    }

    public void setMappedFileSizeConsumeQueue(int mappedFileSizeConsumeQueue) {
        this.mappedFileSizeConsumeQueue = mappedFileSizeConsumeQueue;
    }

    public boolean isEnableConsumeQueueExt() {
        return enableConsumeQueueExt;
    }

    public void setEnableConsumeQueueExt(boolean enableConsumeQueueExt) {
        this.enableConsumeQueueExt = enableConsumeQueueExt;
    }

    public int getMappedFileSizeConsumeQueueExt() {
        return mappedFileSizeConsumeQueueExt;
    }

    public void setMappedFileSizeConsumeQueueExt(int mappedFileSizeConsumeQueueExt) {
        this.mappedFileSizeConsumeQueueExt = mappedFileSizeConsumeQueueExt;
    }

    public int getBitMapLengthConsumeQueueExt() {
        return bitMapLengthConsumeQueueExt;
    }

    public void setBitMapLengthConsumeQueueExt(int bitMapLengthConsumeQueueExt) {
        this.bitMapLengthConsumeQueueExt = bitMapLengthConsumeQueueExt;
    }

    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }

    public void setFlushIntervalCommitLog(int flushIntervalCommitLog) {
        this.flushIntervalCommitLog = flushIntervalCommitLog;
    }

    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }

    public void setFlushIntervalConsumeQueue(int flushIntervalConsumeQueue) {
        this.flushIntervalConsumeQueue = flushIntervalConsumeQueue;
    }

    public int getPutMsgIndexHightWater() {
        return putMsgIndexHightWater;
    }

    public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
        this.putMsgIndexHightWater = putMsgIndexHightWater;
    }

    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }

    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public boolean isCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public boolean getCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
        this.checkCRCOnRecover = checkCRCOnRecover;
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10)
            return 10;

        if (this.diskMaxUsedSpaceRatio > 95)
            return 95;

        return diskMaxUsedSpaceRatio;
    }

    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }

    public int getDeleteCommitLogFilesInterval() {
        return deleteCommitLogFilesInterval;
    }

    public void setDeleteCommitLogFilesInterval(int deleteCommitLogFilesInterval) {
        this.deleteCommitLogFilesInterval = deleteCommitLogFilesInterval;
    }

    public int getDeleteConsumeQueueFilesInterval() {
        return deleteConsumeQueueFilesInterval;
    }

    public void setDeleteConsumeQueueFilesInterval(int deleteConsumeQueueFilesInterval) {
        this.deleteConsumeQueueFilesInterval = deleteConsumeQueueFilesInterval;
    }

    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }

    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }

    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }

    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }

    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }

    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }

    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }

    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }

    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }

    public void setFlushCommitLogLeastPages(int flushCommitLogLeastPages) {
        this.flushCommitLogLeastPages = flushCommitLogLeastPages;
    }

    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }

    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }

    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }

    public void setFlushCommitLogThoroughInterval(int flushCommitLogThoroughInterval) {
        this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
    }

    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }

    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }

    public int getDestroyMapedFileIntervalForcibly() {
        return destroyMapedFileIntervalForcibly;
    }

    public void setDestroyMapedFileIntervalForcibly(int destroyMapedFileIntervalForcibly) {
        this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
    }

    public int getFileReservedTime() {
        return fileReservedTime;
    }

    public void setFileReservedTime(int fileReservedTime) {
        this.fileReservedTime = fileReservedTime;
    }

    public int getRedeleteHangedFileInterval() {
        return redeleteHangedFileInterval;
    }

    public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
        this.redeleteHangedFileInterval = redeleteHangedFileInterval;
    }

    public int getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }

    public void setAccessMessageInMemoryMaxRatio(int accessMessageInMemoryMaxRatio) {
        this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
    }

    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }

    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }

    public int getMaxHashSlotNum() {
        return maxHashSlotNum;
    }

    public void setMaxHashSlotNum(int maxHashSlotNum) {
        this.maxHashSlotNum = maxHashSlotNum;
    }

    public int getMaxIndexNum() {
        return maxIndexNum;
    }

    public void setMaxIndexNum(int maxIndexNum) {
        this.maxIndexNum = maxIndexNum;
    }

    public int getMaxMsgsNumBatch() {
        return maxMsgsNumBatch;
    }

    public void setMaxMsgsNumBatch(int maxMsgsNumBatch) {
        this.maxMsgsNumBatch = maxMsgsNumBatch;
    }

    public int getHaListenPort() {
        return haListenPort;
    }

    public void setHaListenPort(int haListenPort) {
        this.haListenPort = haListenPort;
    }

    public int getHaSendHeartbeatInterval() {
        return haSendHeartbeatInterval;
    }

    public void setHaSendHeartbeatInterval(int haSendHeartbeatInterval) {
        this.haSendHeartbeatInterval = haSendHeartbeatInterval;
    }

    public int getHaHousekeepingInterval() {
        return haHousekeepingInterval;
    }

    public void setHaHousekeepingInterval(int haHousekeepingInterval) {
        this.haHousekeepingInterval = haHousekeepingInterval;
    }

    public BrokerRole getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = BrokerRole.valueOf(brokerRole);
    }

    public int getHaTransferBatchSize() {
        return haTransferBatchSize;
    }

    public void setHaTransferBatchSize(int haTransferBatchSize) {
        this.haTransferBatchSize = haTransferBatchSize;
    }

    public int getHaSlaveFallbehindMax() {
        return haSlaveFallbehindMax;
    }

    public void setHaSlaveFallbehindMax(int haSlaveFallbehindMax) {
        this.haSlaveFallbehindMax = haSlaveFallbehindMax;
    }

    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    public void setFlushDiskType(String type) {
        this.flushDiskType = FlushDiskType.valueOf(type);
    }

    public int getSyncFlushTimeout() {
        return syncFlushTimeout;
    }

    public void setSyncFlushTimeout(int syncFlushTimeout) {
        this.syncFlushTimeout = syncFlushTimeout;
    }

    public String getHaMasterAddress() {
        return haMasterAddress;
    }

    public void setHaMasterAddress(String haMasterAddress) {
        this.haMasterAddress = haMasterAddress;
    }

    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }

    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }

    public long getFlushDelayOffsetInterval() {
        return flushDelayOffsetInterval;
    }

    public void setFlushDelayOffsetInterval(long flushDelayOffsetInterval) {
        this.flushDelayOffsetInterval = flushDelayOffsetInterval;
    }

    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }

    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }

    public boolean isMessageIndexSafe() {
        return messageIndexSafe;
    }

    public void setMessageIndexSafe(boolean messageIndexSafe) {
        this.messageIndexSafe = messageIndexSafe;
    }

    public boolean isFlushCommitLogTimed() {
        return flushCommitLogTimed;
    }

    public void setFlushCommitLogTimed(boolean flushCommitLogTimed) {
        this.flushCommitLogTimed = flushCommitLogTimed;
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public int getFlushLeastPagesWhenWarmMapedFile() {
        return flushLeastPagesWhenWarmMapedFile;
    }

    public void setFlushLeastPagesWhenWarmMapedFile(int flushLeastPagesWhenWarmMapedFile) {
        this.flushLeastPagesWhenWarmMapedFile = flushLeastPagesWhenWarmMapedFile;
    }

    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }

    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }

    public int getDefaultQueryMaxNum() {
        return defaultQueryMaxNum;
    }

    public void setDefaultQueryMaxNum(int defaultQueryMaxNum) {
        this.defaultQueryMaxNum = defaultQueryMaxNum;
    }

    /**
     * Enable transient commitLog store pool only if transientStorePoolEnable is true and the FlushDiskType is
     * ASYNC_FLUSH
     *
     * @return <tt>true</tt> or <tt>false</tt>
     */
    public boolean isTransientStorePoolEnable() {
        return transientStorePoolEnable && FlushDiskType.ASYNC_FLUSH == getFlushDiskType()
                && BrokerRole.SLAVE != getBrokerRole();
    }

    public void setTransientStorePoolEnable(final boolean transientStorePoolEnable) {
        this.transientStorePoolEnable = transientStorePoolEnable;
    }

    public int getTransientStorePoolSize() {
        return transientStorePoolSize;
    }

    public void setTransientStorePoolSize(final int transientStorePoolSize) {
        this.transientStorePoolSize = transientStorePoolSize;
    }

    public int getCommitIntervalCommitLog() {
        return commitIntervalCommitLog;
    }

    public void setCommitIntervalCommitLog(final int commitIntervalCommitLog) {
        this.commitIntervalCommitLog = commitIntervalCommitLog;
    }

    public boolean isFastFailIfNoBufferInStorePool() {
        return fastFailIfNoBufferInStorePool;
    }

    public void setFastFailIfNoBufferInStorePool(final boolean fastFailIfNoBufferInStorePool) {
        this.fastFailIfNoBufferInStorePool = fastFailIfNoBufferInStorePool;
    }

    public boolean isUseReentrantLockWhenPutMessage() {
        return useReentrantLockWhenPutMessage;
    }

    public void setUseReentrantLockWhenPutMessage(final boolean useReentrantLockWhenPutMessage) {
        this.useReentrantLockWhenPutMessage = useReentrantLockWhenPutMessage;
    }

    public int getCommitCommitLogLeastPages() {
        return commitCommitLogLeastPages;
    }

    public void setCommitCommitLogLeastPages(final int commitCommitLogLeastPages) {
        this.commitCommitLogLeastPages = commitCommitLogLeastPages;
    }

    public int getCommitCommitLogThoroughInterval() {
        return commitCommitLogThoroughInterval;
    }

    public void setCommitCommitLogThoroughInterval(final int commitCommitLogThoroughInterval) {
        this.commitCommitLogThoroughInterval = commitCommitLogThoroughInterval;
    }

    public String getdLegerGroup() {
        return dLegerGroup;
    }

    public void setdLegerGroup(String dLegerGroup) {
        this.dLegerGroup = dLegerGroup;
    }

    public String getdLegerPeers() {
        return dLegerPeers;
    }

    public void setdLegerPeers(String dLegerPeers) {
        this.dLegerPeers = dLegerPeers;
    }

    public String getdLegerSelfId() {
        return dLegerSelfId;
    }

    public void setdLegerSelfId(String dLegerSelfId) {
        this.dLegerSelfId = dLegerSelfId;
    }

    public boolean isEnableDLegerCommitLog() {
        return enableDLegerCommitLog;
    }

    public void setEnableDLegerCommitLog(boolean enableDLegerCommitLog) {
        this.enableDLegerCommitLog = enableDLegerCommitLog;
    }
}
