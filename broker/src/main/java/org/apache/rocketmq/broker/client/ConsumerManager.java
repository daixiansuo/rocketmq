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

import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * ConsumerManager用于管理订阅某个Topic的所有Consumer信息，包括Consumer所属的ConsumerGroup、消费方式（广播或集群）、消息模式、消费偏移量等信息，
 * 同时也负责Consumer的注册、注销和心跳检测等工作。ConsumerManager会周期性地扫描所有连接，如果发现有长时间未活动的连接，
 * 就会将其从ConsumerTable中删除。此外，ConsumerManager还提供了查询某个Topic被哪些Consumer订阅的功能。
 */
public class ConsumerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // 表示消费者连接通道过期超时时间，单位为毫秒。默认为2分钟，如果消费者超过这个时间没有发送心跳，则认为连接过期失效
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;

    // 用于保存消费者组信息的ConcurrentHashMap，以消费者组名作为键，对应的消费者组信息为值
    private final ConcurrentMap<String/* Group */, ConsumerGroupInfo> consumerTable =
            new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);

    // 用于监听消费者变化的接口，当有新的消费者加入或退出消费者组时，可以通过该接口实现相应的处理逻辑
    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }


    /**
     * 根据 消费者分组、客户端ID 查询客户端通道信息
     *
     * @param group    消费者分组
     * @param clientId 客户端ID
     * @return ClientChannelInfo
     */
    public ClientChannelInfo findChannel(final String group, final String clientId) {

        // 根据 group 查询对应 消费者分组信息
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            // 根据 clientId 获取对应的 客户端通道信息
            return consumerGroupInfo.findChannel(clientId);
        }
        return null;
    }

    /**
     * 根据 消费者分组、主题 查询订阅数据
     *
     * @param group 消费者分组
     * @param topic 主题
     * @return SubscriptionData
     */
    public SubscriptionData findSubscriptionData(final String group, final String topic) {

        // 根据 group 查询对应 消费者分组信息
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            // 根据 topic 查询订阅信息
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }


    /**
     * 查询订阅信息数据条数
     *
     * @param group 消费者分组
     * @return int
     */
    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            // 获取订阅数据集合大小
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    /**
     * 在消费者断开连接时处理事件，关闭与客户端的连接，并通知消费者组的状态改变。
     * <p>
     * invoke： org.apache.rocketmq.broker.client.ClientHousekeepingService
     *
     * @param remoteAddr 远程地址
     * @param channel    通道
     */
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {

        // 遍历消费者分组信息
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            // 消费者分组
            ConsumerGroupInfo info = next.getValue();
            // 处理通道关闭事件
            boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
            // 如果消费者组中的通道信息被移除
            if (removed) {
                // 如果消费者组的所有通道都被移除
                if (info.getChannelInfoTable().isEmpty()) {
                    // 从消费者组中移除该消费者
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        // 如果移除成功则打印日志并通知消费者组变更监听器
                        log.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                                next.getKey());
                        // 通知消费者组变更监听器，消费者组注销事件
                        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, next.getKey());
                    }
                }
                // 如果只是某个消费者组的某些通道被移除，则通知消费者组变更监听器
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
            }
        }
    }

    /**
     * 注册消费者
     *
     * @param group                            消费者分组
     * @param clientChannelInfo                客户端通道信息
     * @param consumeType                      消费类型
     * @param messageModel                     消息类型
     * @param consumeFromWhere                 消费位置 从哪里消费
     * @param subList                          订阅列表
     * @param isNotifyConsumerIdsChangedEnable 是否允许通知消费者ID已更改
     * @return boolean
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
                                    ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
                                    final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {

        /*
           首先根据消费者组名从消费者表中获取对应的 ConsumerGroupInfo 对象。
           如果对象不存在，则创建一个新的 ConsumerGroupInfo 对象，并将其添加到消费者表中。
         */
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        // 更新消费者的通道信息
        boolean r1 = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                consumeFromWhere);
        // 更新消费者的订阅数据
        boolean r2 = consumerGroupInfo.updateSubscription(subList);

        // 两者又一个更新成功
        if (r1 || r2) {
            // 则根据 isNotifyConsumerIdsChangedEnable 的值决定是否通知消费者ID已更改。
            if (isNotifyConsumerIdsChangedEnable) {
                // 消费者组变更事件
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }

        // 消费者组注册事件
        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, group, subList);

        return r1 || r2;
    }


    /**
     * 在消费者主动断开连接时被调用
     *
     * @param group                            消费者分组
     * @param clientChannelInfo                客户端通道信息
     * @param isNotifyConsumerIdsChangedEnable 是否允许通知消费者ID已更改
     */
    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo,
                                   boolean isNotifyConsumerIdsChangedEnable) {

        // 查询 消费者分组信息
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            // 注销客户端通道信息，是在消费者主动断开连接时被调用
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
            // 注册后，如果客户端通道集合为空，则删除对应的 消费者分组信息
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                // 删除消费者分组信息
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                if (remove != null) {
                    log.info("unregister consumer ok, no any connection, and remove consumer group, {}", group);
                    // 消费者分组注销事件
                    this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, group);
                }
            }

            // 是否允许通知消费者ID已更改
            if (isNotifyConsumerIdsChangedEnable) {
                // 消费者分组变更事件
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
    }


    /**
     * 扫描清除未激活的通道
     */
    public void scanNotActiveChannel() {

        // 遍历消费者分组信息
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            // 消费者分组名称
            String group = next.getKey();
            // 消费者分组信息
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            // 消费者分组 内的客户端通道列表
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                    consumerGroupInfo.getChannelInfoTable();

            // 遍历客户端通道列表
            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                // 客户端通道信息
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                // 计算通道最后一次更新时间与当前时间的差异
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                // 如果超过了阈值，则认为该通道已经过期，并将其关闭。然后它将从通道信息表中删除该通道。
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    log.warn(
                            "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                            RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    // 关闭通道
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    // 移除客户端通道信息
                    itChannel.remove();
                }
            }

            // 如果通道信息表在删除通道后为空，则意味着该消费者组下没有其他活动通道。在这种情况下，该方法将从消费者表中删除该消费者组。
            if (channelInfoTable.isEmpty()) {
                log.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                        group);
                it.remove();
            }
        }
    }

    /**
     * 用于查询订阅了指定主题的消费者组
     *
     * @param topic 主题
     * @return 所有订阅了指定主题的消费者组名称
     */
    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<>();

        // 遍历所有的消费者分组信息
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> entry = it.next();
            ConcurrentMap<String, SubscriptionData> subscriptionTable =
                    entry.getValue().getSubscriptionTable();
            // 检查每个消费者组的订阅数据（subscriptionTable）是否包含指定主题。如果是，则将该消费者组的名称添加到结果集合中。最后返回结果集合。
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }
        return groups;
    }
}
