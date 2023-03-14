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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * 消费者分组信息
 */
public class ConsumerGroupInfo {


    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // 消费者分组名称
    private final String groupName;

    // 订阅信息
    private final ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable =
            new ConcurrentHashMap<String, SubscriptionData>();

    // 存储消费者组中的消费者和它们对应的客户端连接信息。
    // 其中，键为客户端连接 Channel，值为 ClientChannelInfo 对象，用于存储客户端连接的相关信息，如客户端 ID、最近一次发送心跳的时间戳等
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
            new ConcurrentHashMap<Channel, ClientChannelInfo>(16);

    // 消费类型，PULL 或 PUSH
    private volatile ConsumeType consumeType;
    // 消息模式，广播或集群。
    private volatile MessageModel messageModel;
    // 消费位置，从哪里开始消费
    private volatile ConsumeFromWhere consumeFromWhere;
    // 最后更新时间戳，表示该消费者组最后一次更新的时间
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel,
                             ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }


    /**
     * 根据 clientId 查询 客户端通道信息
     *
     * @param clientId 客户端ID
     * @return ClientChannelInfo
     */
    public ClientChannelInfo findChannel(final String clientId) {
        // 遍历所有
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> next = it.next();
            // 比对 clientId
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }

        return null;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public ConcurrentMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }

    /**
     * 获取所有 客户端通道
     *
     * @return List<Channel>
     */
    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }

    /**
     * 获取所有 客户端ID
     *
     * @return List<String>
     */
    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();

        // 遍历所有客户端连接信息
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> entry = it.next();
            ClientChannelInfo clientChannelInfo = entry.getValue();
            // clientId
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }

    /**
     * 取消注册 客户端通道
     * 调用时机： 是在消费者主动断开连接时被调用
     *
     * @param clientChannelInfo 客户端通道信息
     */
    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        // 删除对应的 客户端通道信息
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    /**
     * 用于处理通道关闭事件
     * 调用时机： 是在通道关闭事件触发时被调用。
     *
     * @param remoteAddr 地址
     * @param channel    通道
     * @return boolean
     */
    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        // 删除
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        // 不为空 说明存在，所以返回为 true ！！！
        if (info != null) {
            log.warn(
                    "NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}",
                    info.toString(), groupName);
            return true;
        }

        return false;
    }


    /**
     * 更新通道信息
     *
     * @param infoNew          客户端通道信息
     * @param consumeType      消费类型
     * @param messageModel     消息类型
     * @param consumeFromWhere 消费位置
     * @return boolean
     */
    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType,
                                 MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {

        // 是否更新成功
        boolean updated = false;
        // 更新 消费类型
        this.consumeType = consumeType;
        // 更新 消息类型
        this.messageModel = messageModel;
        // 更新 消费位置
        this.consumeFromWhere = consumeFromWhere;

        // 获取 客户端通道信息
        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        if (null == infoOld) {
            // 添加至 客户端通道信息表中
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            // 如果之前不存在，updated 设置为 true
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType,
                        messageModel, infoNew.toString());
                updated = true;
            }

            infoOld = infoNew;
        } else {
            // 客户端ID不同，打印error日志，更新客户端通道信息
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ",
                        this.groupName,
                        infoOld.toString(),
                        infoNew.toString());
                // 更新客户端通道信息
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }

        // 设置最后更新时间
        this.lastUpdateTimestamp = System.currentTimeMillis();
        // 同样更新
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);

        return updated;
    }

    /**
     * 更新订阅信息
     *
     * @param subList 订阅数据列表
     * @return boolean
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;

        // 遍历订阅数据
        for (SubscriptionData sub : subList) {
            // 根据订阅数据的topic 从缓存获取之前的订阅数据
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            if (old == null) {
                // 不存在则添加
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                if (null == prev) {
                    // 更新状态设置为 true
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}",
                            this.groupName,
                            sub.toString());
                }

            } else if (sub.getSubVersion() > old.getSubVersion()) { // 存在旧数据，且订阅版本大于之前的订阅版本，则更新本地订阅信息

                // PUSH模式 打印日志
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}",
                            this.groupName,
                            old.toString(),
                            sub.toString()
                    );
                }

                // 更新本地订阅数据
                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }

        // 遍历 新的订阅数据集合
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();

            /**
             * 再次遍历所有订阅数据，如果新的订阅信息集合中不包含该订阅信息，则从缓存中移除该订阅信息，
             * 并将更新状态设置为 true。最后将更新时间设置为当前时间，返回更新状态。
             */
            boolean exist = false;
            for (SubscriptionData sub : subList) {
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            if (!exist) {
                log.warn("subscription changed, group: {} remove topic {} {}",
                        this.groupName,
                        oldTopic,
                        next.getValue().toString()
                );

                it.remove();
                updated = true;
            }
        }

        // 设置消费者最后更新时间
        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }

    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }


    /**
     * 根据 topic 查询订阅信息
     *
     * @param topic 主题
     * @return SubscriptionData
     */
    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getGroupName() {
        return groupName;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
