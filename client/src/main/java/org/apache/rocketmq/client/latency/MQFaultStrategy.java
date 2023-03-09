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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略，延迟实现的门面类
 * <p>
 * 开启所谓的故障延迟机制，即设置sendLatencyFaultEnable为ture， 其实是一种较为悲观的做法。当消息发送者遇到一次消息发送失败后，
 * 就会悲观地认为Broker不可用，在接下来的一段时间内就不再向 其发送消息，直接避开该Broker。
 * 而不开启延迟规避机制，就只会在 本次消息发送的重试过程中规避该Broker，下一次消息发送还是会继 续尝试！！！
 */
public class MQFaultStrategy {

    private final static InternalLogger log = ClientLogger.getLog();

    // 失败延迟机制
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    // 默认不启用 Broker 故障延迟机制
    private boolean sendLatencyFaultEnable = false;

    // 消息发送故障的延迟时间
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    // 不可用持续时长
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {

        // 是否启用 Broker 故障延迟机制，默认不启用
        if (this.sendLatencyFaultEnable) {
            try {

                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                // 遍历消息队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    // 取模
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    // 获取队列
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 关键：验证该消息队列是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        // 上次发送失败broker为空 或者 当前队列的broker = 上次发送失败broker ；
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            // 兜底方案，使用主题发布信息进行选择队列。
            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * @param brokerName     broker名称
     * @param currentLatency 本次消息发送的延迟时间
     * @param isolation      是否规避Broker，该参数如果为true，则使用默认时 长30s来计算Broker故障规避时长，
     *                       如果为false，则使用本次消 息发送延迟时间来计算Broker故障规避时长
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 启用Broker故障延迟机制，才去更新失败条目
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
