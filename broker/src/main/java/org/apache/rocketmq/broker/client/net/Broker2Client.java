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
package org.apache.rocketmq.broker.client.net;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBodyForC;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * 消费者、生产者连接到 broker 之后，都对应一个 netty channel。
 * 而 Broker2Client 作用，主要是通过 nettyRemoteServer 向连接到 broker 的 client (消费者、生产者) 发送消息。
 */
public class Broker2Client {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // 相互持有
    private final BrokerController brokerController;

    public Broker2Client(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 检查生产者事务状态
     *
     * @param group         生产者分组
     * @param channel       可用的生产者通道
     * @param requestHeader 检查事务状态请求的头部信息
     * @param messageExt    消息实体对象
     */
    public void checkProducerTransactionState(
            final String group,
            final Channel channel,
            final CheckTransactionStateRequestHeader requestHeader,
            final MessageExt messageExt) throws Exception {

        // 构建请求，设置请求CODE
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
        // 设置请求体内容
        request.setBody(MessageDecoder.encode(messageExt, false));
        try {
            // 由 nettyServer 向 producer 单向发送请求，不需要响应结果。
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("Check transaction failed because invoke producer exception. group={}, msgId={}", group, messageExt.getMsgId(), e.getMessage());
        }
    }


    /**
     * 向指定的客户端发送请求并等待响应
     *
     * @param channel 客户端对应的 Netty通道
     * @param request 请求对象
     * @return RemotingCommand
     */
    public RemotingCommand callClient(final Channel channel,
                                      final RemotingCommand request) throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        // 超时时间 10s，其实就是 server（broker） 回调 client（producer、consumer）。
        return this.brokerController.getRemotingServer().invokeSync(channel, request, 10000);
    }


    /**
     * 在 RocketMQ 中，每个消费者都会被分配一个消费者 ID，该 ID 用于标识唯一的消费者。当消费者启动或停止时，其消费者 ID 就会发生变化。
     * 这个方法会发送一个通知消息给客户端，告知其所连接的 Broker 上的指定消费者组的消费者 ID 发生了变化。
     * 这样，客户端就可以及时更新其所维护的消费者 ID 信息，以保证消费者组中的所有消费者状态正确
     * <p>
     * invoke：ConsumerIdsChangeListener 的默认实现类回调。
     *
     * @param channel       客户端netty通道
     * @param consumerGroup 消费者分组
     */
    public void notifyConsumerIdsChanged(
            final Channel channel,
            final String consumerGroup) {
        if (null == consumerGroup) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        // 构建请求
        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);

        try {
            // 发送单向请求
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e.getMessage());
        }
    }

    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce) {
        return resetOffset(topic, group, timeStamp, isForce, false);
    }


    /**
     * 重置指定消费者组在特定主题上的消费进度（消费位点）
     * <p>
     * 先确认需要重置的偏移量，然后向所有消费者实例发送reset请求。
     *
     * @param topic     主题
     * @param group     消费者分组
     * @param timeStamp 时间戳， 如果不等-1，则表示使用指定的时间戳来确定重置的消费进度。
     * @param isForce   是否强制重置
     * @param isC       是否为 C 语言
     * @return RemotingCommand
     */
    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce, boolean isC) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 根据 topic 获取主题配置
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            log.error("[reset-offset] reset offset failed, no topic in this broker. topic={}", topic);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("[reset-offset] reset offset failed, no topic in this broker. topic=" + topic);
            return response;
        }

        // 将要重置消费进度表 ！！！
        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();

        // 遍历主题上的 可写队列数量
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {

            // 组装队列模型
            MessageQueue mq = new MessageQueue();
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setTopic(topic);
            mq.setQueueId(i);

            // 查询当前的消费进度
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, i);
            if (-1 == consumerOffset) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("THe consumer group <%s> not exist", group));
                return response;
            }

            long timeStampOffset;
            // 如果时间戳为 -1，则表示使用当前时间来确定消费进度。如果时间戳不为 -1，则表示使用指定的时间戳来确定消费进度
            if (timeStamp == -1) {
                // 获取指定 topic、queue 的最大消息位点。
                timeStampOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            } else {
                // 根据时间戳 获取指定 topic、queue 的消息位点。
                timeStampOffset = this.brokerController.getMessageStore().getOffsetInQueueByTime(topic, i, timeStamp);
            }

            if (timeStampOffset < 0) {
                log.warn("reset offset is invalid. topic={}, queueId={}, timeStampOffset={}", topic, i, timeStampOffset);
                timeStampOffset = 0;
            }

            /**
             * 如果时间戳对应的消费进度小于等于当前的消费进度，则不会更新消费进度
             *
             * 个人理解：
             *  首先是根据 timestamp 拿到一个 "重置的偏移量 = timeStampOffset "。
             *  其次，判断 timeStampOffset 如果小于 当前消费进度，则将消费进度重置为 timeStampOffset。
             *  如果 timeStampOffset 大于 当前消费进度，则保持原来的消费进度（大于意味着要 跳过一段消息进行消费）。
             *  但是，如果是强制重置，不管你是大于小于，直接将消费进度重置为 timeStampOffset ！！
             */
            if (isForce || timeStampOffset < consumerOffset) {
                offsetTable.put(mq, timeStampOffset);
            } else {
                offsetTable.put(mq, consumerOffset);
            }
        }

        // 请求头信息
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timeStamp);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, requestHeader);

        // 是否为 c++ language
        if (isC) {
            // c++ language
            ResetOffsetBodyForC body = new ResetOffsetBodyForC();
            List<MessageQueueForC> offsetList = convertOffsetTable2OffsetList(offsetTable);
            body.setOffsetTable(offsetList);
            request.setBody(body.encode());
        } else {
            // other language
            // 请求体
            ResetOffsetBody body = new ResetOffsetBody();
            body.setOffsetTable(offsetTable);
            request.setBody(body.encode());
        }

        // 获取消费者分组信息
        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(group);
        // 为空，则表明消费者不在线，无法进行重置处理/
        if (consumerGroupInfo != null && !consumerGroupInfo.getAllChannel().isEmpty()) {

            // 获取消费者通道表
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                    consumerGroupInfo.getChannelInfoTable();
            // 遍历所有消费者通道
            for (Map.Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
                // 客户端版本
                int version = entry.getValue().getVersion();
                // 支持 3.0.7 以上版本
                if (version >= MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                    try {
                        // 向消费者 发送单向 重置请求！
                        this.brokerController.getRemotingServer().invokeOneway(entry.getKey(), request, 5000);
                        log.info("[reset-offset] reset offset success. topic={}, group={}, clientId={}",
                                topic, group, entry.getValue().getClientId());
                    } catch (Exception e) {
                        log.error("[reset-offset] reset offset exception. topic={}, group={}",
                                new Object[]{topic, group}, e);
                    }
                } else {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("the client does not support this feature. version="
                            + MQVersion.getVersionDesc(version));
                    log.warn("[reset-offset] the client does not support this feature. version={}",
                            RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                    return response;
                }
            }
        } else {
            String errorInfo =
                    String.format("Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d",
                            requestHeader.getGroup(),
                            requestHeader.getTopic(),
                            requestHeader.getTimestamp());
            log.error(errorInfo);
            response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
            response.setRemark(errorInfo);
            return response;
        }

        // 组装返回数据
        response.setCode(ResponseCode.SUCCESS);
        ResetOffsetBody resBody = new ResetOffsetBody();
        resBody.setOffsetTable(offsetTable);
        response.setBody(resBody.encode());
        return response;
    }

    private List<MessageQueueForC> convertOffsetTable2OffsetList(Map<MessageQueue, Long> table) {
        List<MessageQueueForC> list = new ArrayList<>();
        for (Entry<MessageQueue, Long> entry : table.entrySet()) {
            MessageQueue mq = entry.getKey();
            MessageQueueForC tmp =
                    new MessageQueueForC(mq.getTopic(), mq.getBrokerName(), mq.getQueueId(), entry.getValue());
            list.add(tmp);
        }
        return list;
    }


    /**
     * 获取 消费者 在指定 topic 下的消费状态
     *
     * @param topic          主题
     * @param group          消费者分组
     * @param originClientId 原来的客户端ID，如果为空则获取消费者分组下所有消费者的消费状态， 如果不为空则查询指定的消费者状态。
     * @return RemotingCommand
     */
    public RemotingCommand getConsumeStatus(String topic, String group, String originClientId) {
        final RemotingCommand result = RemotingCommand.createResponseCommand(null);

        // 请求头信息
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT,
                        requestHeader);

        Map<String, Map<MessageQueue, Long>> consumerStatusTable =
                new HashMap<String, Map<MessageQueue, Long>>();

        // 获取消费者分组下的 所有消费者通道信息
        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(group).getChannelInfoTable();
        // 如果为空，直接返回系统异常
        if (null == channelInfoTable || channelInfoTable.isEmpty()) {
            result.setCode(ResponseCode.SYSTEM_ERROR);
            result.setRemark(String.format("No Any Consumer online in the consumer group: [%s]", group));
            return result;
        }

        // 遍历所有消费者
        for (Map.Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {

            // 客户端版本
            int version = entry.getValue().getVersion();
            String clientId = entry.getValue().getClientId();
            // 校验判断是否合法
            if (version < MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                result.setCode(ResponseCode.SYSTEM_ERROR);
                result.setRemark("the client does not support this feature. version="
                        + MQVersion.getVersionDesc(version));
                log.warn("[get-consumer-status] the client does not support this feature. version={}",
                        RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                return result;
            } else if (UtilAll.isBlank(originClientId) || originClientId.equals(clientId)) {
                try {

                    // 发送请求查询状态
                    RemotingCommand response =
                            this.brokerController.getRemotingServer().invokeSync(entry.getKey(), request, 5000);
                    assert response != null;
                    switch (response.getCode()) {
                        case ResponseCode.SUCCESS: {
                            if (response.getBody() != null) {
                                GetConsumerStatusBody body =
                                        GetConsumerStatusBody.decode(response.getBody(),
                                                GetConsumerStatusBody.class);

                                consumerStatusTable.put(clientId, body.getMessageQueueTable());
                                log.info(
                                        "[get-consumer-status] get consumer status success. topic={}, group={}, channelRemoteAddr={}",
                                        topic, group, clientId);
                            }
                        }
                        default:
                            break;
                    }
                } catch (Exception e) {
                    log.error(
                            "[get-consumer-status] get consumer status exception. topic={}, group={}, offset={}",
                            new Object[]{topic, group}, e);
                }

                if (!UtilAll.isBlank(originClientId) && originClientId.equals(clientId)) {
                    break;
                }
            }
        }

        result.setCode(ResponseCode.SUCCESS);
        GetConsumerStatusBody resBody = new GetConsumerStatusBody();
        resBody.setConsumerTable(consumerStatusTable);
        result.setBody(resBody.encode());
        return result;
    }
}
