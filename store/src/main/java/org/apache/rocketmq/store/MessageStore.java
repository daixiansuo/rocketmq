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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * 此类定义要实现的协定接口，允许第三方供应商使用自定义的消息存储库。
 * This class defines contracting interfaces to implement, allowing third-party vendor to use customized message store.
 */
public interface MessageStore {

    /**
     * 加载以前的消息
     * <p>
     * Load previously stored messages.
     *
     * @return true if success; false otherwise.
     */
    boolean load();

    /**
     * 启动消息存储
     * Launch this message store.
     *
     * @throws Exception if there is any error.
     */
    void start() throws Exception;

    /**
     * 关闭消息存储
     * Shutdown this message store.
     */
    void shutdown();

    /**
     * 销毁此消息存储库。通常，应在调用后删除所有持久文件。
     * Destroy this message store. Generally, all persistent files should be removed after invocation.
     */
    void destroy();

    /**
     * 将消息存储到存储中。
     * Store a message into store.
     *
     * @param msg Message instance to store
     * @return result of store operation.
     */
    PutMessageResult putMessage(final MessageExtBrokerInner msg);

    /**
     * 存储一批消息。
     * Store a batch of messages.
     *
     * @param messageExtBatch Message batch.
     * @return result of storing batch messages.
     */
    PutMessageResult putMessages(final MessageExtBatch messageExtBatch);

    /**
     * 用于从指定消费者组的指定主题、队列和偏移量处开始查询消息的方法
     * <p>
     * Query at most <code>maxMsgNums</code> messages belonging to <code>topic</code> at <code>queueId</code> starting
     * from given <code>offset</code>. Resulting messages will further be screened using provided message filter.
     *
     * @param group         Consumer group that launches this query.                消费者组名称。
     * @param topic         Topic to query.                                         消息主题。
     * @param queueId       Queue ID to query.                                      队列编号
     * @param offset        Logical offset to start from.                           从该偏移量开始查询。
     * @param maxMsgNums    Maximum count of messages to query.                     最多查询的消息数量。
     * @param messageFilter Message filter used to screen desired messages.         用于筛选所需消息的消息过滤器。
     * @return Matched messages.
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * 获取指定主题、队列下的 最大消息位点
     * Get maximum offset of the topic queue.
     *
     * @param topic   Topic name.
     * @param queueId Queue ID.
     * @return Maximum offset at present.
     */
    long getMaxOffsetInQueue(final String topic, final int queueId);

    /**
     * 获取指定主题、队列下的 最小消息位点
     * Get the minimum offset of the topic queue.
     *
     * @param topic   Topic name.
     * @param queueId Queue ID.
     * @return Minimum offset at present.
     */
    long getMinOffsetInQueue(final String topic, final int queueId);

    /**
     * 用于获取消费队列中某个消息对应的 CommitLog 的物理偏移量（commitLogOffset）。
     * <p>
     * Get the offset of the message in the commit log, which is also known as physical offset.
     *
     * @param topic              Topic of the message to lookup.
     * @param queueId            Queue ID.
     * @param consumeQueueOffset offset of consume queue.
     * @return physical offset.  方法返回值为 commitLogOffset，即消息在 CommitLog 中的物理偏移量。
     */
    long getCommitLogOffsetInQueue(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * 通过指定的时间戳查找消息在队列中的物理偏移量，返回值是该时间戳对应的消息的物理偏移量
     * <p>
     * Look up the physical offset of the message whose store timestamp is as specified.
     *
     * @param topic     Topic of the message.
     * @param queueId   Queue ID.
     * @param timestamp Timestamp to look up.
     * @return physical offset which matches.
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);

    /**
     * 通过指定消息的物理偏移量(commitLogOffset)来查找相应的消息(MessageExt)。
     * <p>
     * 消息的物理偏移量是指消息在消息存储文件中的起始位置。
     *
     * @param commitLogOffset physical offset.
     * @return Message whose physical offset is as specified.
     */
    MessageExt lookMessageByOffset(final long commitLogOffset);

    /**
     * 该方法根据指定的Commit Log Offset（物理偏移量），从Commit Log中获取对应的消息，并返回消息的封装结果。
     * <p>
     * 具体来说，该方法会首先根据commitLogOffset获取消息存储所在的文件（MappedFile），
     * 然后调用MappedFile#selectMappedBuffer方法获取消息存储所在的内存映射ByteBuffer，
     * 接着根据Commit Log的存储格式从ByteBuffer中解析出对应的消息，并返回封装结果
     * <p>
     * <p>
     * Get one message from the specified commit log offset.
     *
     * @param commitLogOffset commit log offset.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset);

    /**
     * 是从CommitLog中根据消息的物理偏移量和消息大小获取一条消息的数据.
     * <p>
     * Get one message from the specified commit log offset.
     *
     * @param commitLogOffset commit log offset.
     * @param msgSize         message size.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset, final int msgSize);

    /**
     * 用于获取当前消息存储的运行状态信息的
     * <p>
     * Get the running information of this store.
     *
     * @return message store running info.
     */
    String getRunningDataInfo();

    /**
     * Message store runtime information, which should generally contains various statistical information.
     *
     * @return runtime information of the message store in format of key-value pairs.
     */
    HashMap<String, String> getRuntimeInfo();

    /**
     * 该方法是获取commit log中最大的物理offset
     * <p>
     * 在RocketMQ中，消息发送到Broker后会先写入commit log，再写入consume queue。
     * 这里的物理offset指的是消息在commit log文件中的位置，通过获取最大的物理offset可以得知commit log文件的大小和剩余空间等信息。
     * <p>
     * Get the maximum commit log offset.
     *
     * @return maximum commit log offset.
     */
    long getMaxPhyOffset();

    /**
     * 获取消息存储中最小的消息物理偏移量（commit log offset）。
     * <p>
     * Get the minimum commit log offset.
     *
     * @return minimum commit log offset.
     */
    long getMinPhyOffset();

    /**
     * 该方法返回给定队列中最早一条消息的存储时间。即在指定主题和队列ID的队列中，返回存储时间最早的消息的时间戳。
     * <p>
     * Get the store time of the earliest message in the given queue.
     *
     * @param topic   Topic of the messages to query.
     * @param queueId Queue ID to find.
     * @return store time of the earliest message.
     */
    long getEarliestMessageTime(final String topic, final int queueId);

    /**
     * 这个方法可以获取整个消息存储中最早一条消息的存储时间戳。它没有传入特定的 Topic 和 Queue ID，而是返回存储中所有消息的最早时间戳。
     * <p>
     * Get the store time of the earliest message in this store.
     *
     * @return timestamp of the earliest message in this store.
     */
    long getEarliestMessageTime();

    /**
     * 该方法返回指定消息在存储中的时间戳。需要传入消息的主题（topic）、队列ID（queueId）以及消费队列的偏移量（consumeQueueOffset）。
     * <p>
     * Get the store time of the message specified.
     *
     * @param topic              message topic.
     * @param queueId            queue ID.
     * @param consumeQueueOffset consume queue offset.
     * @return store timestamp of the message.
     */
    long getMessageStoreTimeStamp(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * 用于获取指定队列中消息的总数量
     * <p>
     * Get the total number of the messages in the specified queue.
     *
     * @param topic   Topic
     * @param queueId Queue ID.
     * @return total number.
     */
    long getMessageTotalInQueue(final String topic, final int queueId);

    /**
     * 方法用于获取从给定偏移量开始的原始 commit log 数据，通常用于复制目的
     * <p>
     * Get the raw commit log data starting from the given offset, which should used for replication purpose.
     *
     * @param offset starting offset.
     * @return commit log data.
     */
    SelectMappedBufferResult getCommitLogData(final long offset);

    /**
     * 方法是将数据追加到 commit log 中，其中 startOffset 是追加的起始偏移量，data 是要追加的数据。方法会返回一个布尔值，表示追加是否成功。
     * <p>
     * Append data to commit log.
     *
     * @param startOffset starting offset.
     * @param data        data to append.
     * @return true if success; false otherwise.
     */
    boolean appendToCommitLog(final long startOffset, final byte[] data);

    /**
     * 方法用于手动触发文件删除操作。
     * <p>
     * 在 RocketMQ 中，CommitLog 和 ConsumeQueue 文件过多时会触发删除操作以保证系统正常运行。
     * 然而，删除操作并不是实时执行的，而是定期触发的。如果你需要立即删除指定文件，可以调用该方法手动触发删除操作。
     * <p>
     * Execute file deletion manually.
     */
    void executeDeleteFilesManually();

    /**
     * 方法用于根据给定的消息关键字(key)和时间范围(begin和end)查询消息存储中符合条件的消息。
     * 可以指定最大返回消息数(maxNum)。
     * <p>
     * 这个方法可以用于需要根据关键字查询消息的场景，比如消息重发、消息追踪等。
     * 例如，如果某个消费者报告说它没有收到消息，但它知道消息关键字，则可以使用此方法查询消息以了解其发送情况。
     * 或者，在消息追踪系统中，可以使用此方法查询与特定关键字相关的消息。
     * <p>
     * Query messages by given key.
     *
     * @param topic  topic of the message.
     * @param key    message key.
     * @param maxNum maximum number of the messages possible.
     * @param begin  begin timestamp.
     * @param end    end timestamp.
     * @return 返回的结果(QueryMessageResult)包括匹配到的消息列表以及消息总数。
     */
    QueryMessageResult queryMessage(final String topic, final String key, final int maxNum, final long begin, final long end);

    /**
     * 方法用于更新高可用（HA）模式下的主节点地址。
     * <p>
     * 在RocketMQ的HA模式中，主节点（Master）和备份节点（Slave）运行在不同的服务器上，并通过网络进行通信。
     * 如果主节点故障，备份节点会自动接替成为新的主节点。当主节点恢复正常运行时，RocketMQ需要将新的主节点地址通知所有的备份节点。
     * 因此，该方法用于更新备份节点中保存的主节点地址信息。通常情况下，该方法会在主节点故障恢复后被调用。
     * <p>
     * Update HA master address.
     *
     * @param newAddr new address.
     */
    void updateHaMasterAddress(final String newAddr);

    /**
     * 方法用于查询从节点相对于主节点的数据落后情况，返回从节点相对于主节点的落后字节数。
     * <p>
     * 可以用于监控从节点与主节点数据同步的情况，及时发现同步问题并进行处理。
     * <p>
     * Return how much the slave falls behind.
     *
     * @return number of bytes that slave falls behind.
     */
    long slaveFallBehindMuch();

    /**
     * 方法返回存储当前的时间戳
     * <p>
     * Return the current timestamp of the store.
     *
     * @return current time in milliseconds since 1970-01-01.
     */
    long now();

    /**
     * 方法用于清除RocketMQ中没有被使用的Topic（即没有消息发送和消费）。
     * <p>
     * 在调用该方法时，需要传入一个Set类型的参数topics，表示所有有效的Topic集合。
     * 方法会比较这个有效的Topic集合和Broker中存在的Topic集合，如果某个Topic不存在于有效的Topic集合中，那么就会将其删除。该方法会返回删除的Topic的数量。
     * <p>
     * Clean unused topics.
     *
     * @param topics all valid topics.
     * @return number of the topics deleted.
     */
    int cleanUnusedTopic(final Set<String> topics);

    /**
     * 用于清理已过期的消费队列的方法
     * <p>
     * 当消费者不再消费一个特定的队列时，消费队列不会立即被删除。相反，它们会被标记为已过期，然后由定期运行的清理任务清理。
     * 这个方法会清理所有已过期的消费队列。这是一个重要的维护任务，因为它可以释放存储空间并减少元数据的大小。
     * <p>
     * Clean expired consume queues.
     */
    void cleanExpiredConsumerQueue();

    /**
     * 用于检查给定的消息是否已经被交换出内存，即该消息是否存在于磁盘中。
     * <p>
     * 参数包括topic，queueId和consumeOffset。其中，topic和queueId用于确定消息所在的队列，consumeOffset是消费者的消费进度，用于确定消息在队列中的位置。
     * 方法返回一个boolean值，如果消息已经被交换出内存则返回true，否则返回false。
     * <p>
     * 该方法常用于消息检索和查询时，检查消息是否已经被移出内存，如果已经被移出内存则需要从磁盘中读取。
     * <p>
     * Check if the given message has been swapped out of the memory.
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is no longer in memory; false otherwise.
     */
    boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * GPT：用于获取尚未分发到消费队列的数据量大小（以字节为单位），即已经存储在Commit Log中但尚未被消费。
     * 可以通过调用该方法来了解系统中已经积压的消息量，进而进行监控和调优。
     * <p>
     * 翻译：获取已存储在提交日志中但尚未调度到使用队列的字节数。
     * Get number of the bytes that have been stored in commit log and not yet dispatched to consume queue.
     *
     * @return number of the bytes to dispatch.
     */
    long dispatchBehindBytes();

    /**
     * 该方法的作用是将消息存储刷写到持久化存储设备中，确保消息数据的持久性。
     * <p>
     * 方法会将内存中的消息数据全部刷写到磁盘，返回最大的已刷写的消息偏移量。该方法适用于需要手动刷写数据到磁盘的场景，例如数据的同步或备份
     * <p>
     * Flush the message store to persist all data.
     *
     * @return maximum offset flushed to persistent storage device.
     */
    long flush();

    /**
     * 重置消息的写入偏移量，即从指定的物理偏移量处开始写入新的消息
     * <p>
     * 当写入偏移量需要更改时，可以使用该方法。例如，当需要更改生产者的写入位置时，可以通过调用该方法将偏移量设置为新的值。
     * <p>
     * Reset written offset.
     *
     * @param phyOffset new offset.
     * @return true if success; false otherwise.
     */
    boolean resetWriteOffset(long phyOffset);

    /**
     * TODO：存疑 ！！！
     * <p>
     * 方法的作用是获取事务消息的确认偏移量（commit log offset），即已经提交的最后一条事务消息的偏移量。
     * <p>
     * 在RocketMQ中，事务消息的发送者需要等待半消息被成功发送到Broker后，才能进行后续的业务操作，等待期间消息处于半消息状态，
     * 只有被确认提交后才变成可消费的全消息。因此，获取事务消息的确认偏移量对于发送者来说非常重要，可以用来判断当前事务消息是否已经被成功提交。
     * <p>
     * Get confirm offset.
     *
     * @return confirm offset.
     */
    long getConfirmOffset();

    /**
     * 方法的作用是设置消息确认的偏移量，即设置消费者已经消费到的最后一个消息的偏移量。
     * <p>
     * 使用场景是在消息确认机制中，当消费者成功消费了一批消息后，调用该方法设置消费确认偏移量，标识该批消息已经被消费。
     * 这个偏移量将用于后续的消息重复消费检查以及消息存储文件的清理。
     * <p>
     * Set confirm offset.
     *
     * @param phyOffset confirm offset to set.
     */
    void setConfirmOffset(long phyOffset);

    /**
     * 方法用于检查操作系统页面缓存是否繁忙，即是否有过多的I/O请求排队等待缓存或释放页面。
     * <p>
     * 在高负载情况下，操作系统页面缓存可能会变得繁忙，从而降低消息存储和检索的性能。
     * 因此，使用此方法可以帮助系统管理员和开发人员监视系统性能，并确定是否需要采取措施来优化系统性能。
     * 该方法通常用于消息存储和检索系统中。
     * <p>
     * Check if the operation system page cache is busy or not.
     *
     * @return true if the OS page cache is busy; false otherwise.
     */
    boolean isOSPageCacheBusy();

    /**
     * GPT： 方法的作用是获取存储（message store）当前已经被锁住的时间（以毫秒为单位）。
     * <p>
     * 通常，存储被锁住的时间越长，意味着存储正在被使用或者正在处理一些操作，这可能会对存储的性能产生一定的影响。
     * 因此，可以使用该方法来检查存储是否被过度使用，以及检查是否存在性能问题。
     * <p>
     * <p>
     * 翻译：到目前为止，以毫秒为单位获取存储的锁定时间。
     * <p>
     * Get lock time in milliseconds of the store by far.
     *
     * @return lock time in milliseconds.
     */
    long lockTimeMills();

    /**
     * 方法的作用是检查瞬态存储池是否不足。
     * <p>
     * 在消息存储中，RocketMQ使用瞬态存储池（TransientStorePool）来缓存消息，以提高存储的效率。
     * 如果瞬态存储池不足，可能会导致存储延迟或丢失消息。因此，使用该方法可以及时检查瞬态存储池的状态，以保证消息存储的可靠性。
     * <p>
     * Check if the transient store pool is deficient.
     *
     * @return true if the transient store pool is running out; false otherwise.
     */
    boolean isTransientStorePoolDeficient();

    /**
     * 方法用于获取提交日志调度器（CommitLogDispatcher）的列表。
     * <p>
     * 调度器用于将消息从提交日志（commit log）发送到消费队列（consumer queue）中，以便进行消息传递。
     * 在RocketMQ中，一个提交日志有多个调度器来进行并行的消息传递。
     * 因此，使用该方法可以获取所有调度器的列表。常见的使用场景是在需要监控提交日志调度器状态和性能时使用。
     * <p>
     * Get the dispatcher list.
     *
     * @return list of the dispatcher.
     */
    LinkedList<CommitLogDispatcher> getDispatcherList();

    /**
     * 该方法的作用是获取指定主题和队列ID的消费队列
     * <p>
     * 消费队列存储了 消费者消费消息的位置信息以及是否已经被删除等信息，是消息存储与消费的桥梁。
     * 该方法适用于需要操作指定主题和队列ID的消费队列的场景，
     * 比如消费者在消费消息的时候需要更新消费进度，或者管理者需要查看消费队列的状态。
     * <p>
     * Get consume queue of the topic/queue.
     *
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueue getConsumeQueue(String topic, int queueId);

    /**
     * 方法返回与消息存储关联的BrokerStatsManager对象。
     * <p>
     * BrokerStatsManager用于跟踪和记录消息经纪人的各种统计信息，例如消息发送和接收速率，消费速率等。
     * 可以使用此方法获取并检索统计信息，以便于对消息存储进行监控和管理。
     * <p>
     * Get BrokerStatsManager of the messageStore.
     *
     * @return BrokerStatsManager.
     */
    BrokerStatsManager getBrokerStatsManager();

    /**
     * 方法用于处理定时消息服务，根据 BrokerRole 的值来启动或停止定时消息服务。
     * 当 BrokerRole 为 SLAVE 时，停止定时消息服务；
     * 当 BrokerRole 为 MASTER 时，启动定时消息服务。
     * <p>
     * 使用场景：在消息服务启动时，通过该方法启动定时消息服务，以便在需要时，消息服务可以将定时消息发送到消费者。
     * 在切换 BrokerRole 时，通过该方法来控制定时消息服务的启动和停止。
     *
     * @param brokerRole master or slave
     */
    void handleScheduleMessageService(BrokerRole brokerRole);
}
