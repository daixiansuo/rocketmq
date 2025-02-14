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
package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Base interface for MQ management
 * <p>
 * MQ管理的基本接口，其下有三个不同子接口：
 * <p>
 * 1. MQAdminExt：这个位于 tools 包下，为 mqadmin 命令的顶级接口，该包就是 mqadmin 命令的功能实现。
 * 2. MQProducer: 消息生产者
 * 3. MQConsumer: 消息消费者
 */
public interface MQAdmin {
    /**
     * 创建主题
     *
     * @param key      accesskey 访问key，4.6.0版本目前无实际作用，可以与 newTopic 相同。
     * @param newTopic topic name 主题名称
     * @param queueNum topic's queue number 队列数量
     */
    void createTopic(final String key, final String newTopic, final int queueNum)
            throws MQClientException;

    /**
     * 创建主题
     *
     * @param key          accesskey
     * @param newTopic     topic name 主题名称
     * @param queueNum     topic's queue number 队列数量
     * @param topicSysFlag topic system flag 主题系统标签，默认为0
     */
    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException;

    /**
     * Gets the message queue offset according to some time in milliseconds<br>
     * be cautious to call because of more IO overhead
     * <p>
     * 根据时间戳从队列中查找其偏移量
     *
     * @param mq        Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return offset
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

    /**
     * Gets the max offset
     * <p>
     * 查找该消息队列中最大的物理偏移量
     *
     * @param mq Instance of MessageQueue
     * @return the max offset
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;

    /**
     * Gets the minimum offset
     * <p>
     * 查找该消息队列中的最小物理偏移量
     *
     * @param mq Instance of MessageQueue
     * @return the minimum offset
     */
    long minOffset(final MessageQueue mq) throws MQClientException;

    /**
     * Gets the earliest stored message time
     * 最早被存储消息的时间
     *
     * @param mq Instance of MessageQueue
     * @return the time in microseconds
     */
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;

    /**
     * Query message according to message id
     * <p>
     * 根据消息偏移量查找消息
     *
     * @param offsetMsgId message id
     * @return message
     */
    MessageExt viewMessage(final String offsetMsgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;

    /**
     * Query messages
     * <p>
     * 根据条件查询消息
     *
     * @param topic  message topic 消息主题
     * @param key    message key index word 消息索引字段
     * @param maxNum max message number 本次最多取出的消息条数
     * @param begin  from when 开始时间
     * @param end    to when 结束时间
     * @return Instance of QueryResult
     */
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
                             final long end) throws MQClientException, InterruptedException;

    /**
     * 根据主题与消息ID查找消息
     *
     * @return The {@code MessageExt} of given msgId
     */
    MessageExt viewMessage(String topic,
                           String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}