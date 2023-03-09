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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {

    // 是否为顺序 主题/消息
    private boolean orderTopic = false;
    // 是否有路由信息
    private boolean haveTopicRouterInfo = false;

    // 队列列表
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    // 每选择一次消息队列，该值会自增1，如果超过 Integer.MAX_VALUE，则重置为0，用于选择消息队列
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    // 路由信息
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }


    /**
     * 选择发送队列 ！！！
     *
     * @param lastBrokerName 上一次选择的执行发送消息失败的Broker
     * @return MessageQueue
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {

        /**
         * 或许有读者会问，Broker不可用后，路由信息中为什么还会包含 该Broker的路由信息呢?其实这不难解释:
         * <p>
         * 首先，NameServer检测 Broker是否可用是有延迟的，最短为一次心跳检测间隔(10s);
         * 其次，NameServer不是检测到Broker宕机后马上推送消息给消息生产者，而是消息生产者每隔30s更新一次路由信息，
         * 因此消息生产者最快 感知Broker最新的路由信息也需要30s。
         * 这就需要引入一种机制，在 Broker宕机期间，一次消息发送失败后，将该Broker暂时排除在消息 队列的选择范围中
         */

        // lastBrokerName 上一次选择的执行发送消息失败的Broker， 如果 lastBrokerName 为 null，则表示这是第一次 select ！！！
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int pos = Math.abs(index++) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                // 选择队列
                MessageQueue mq = this.messageQueueList.get(pos);
                // TODO：选择的队列所属的 broker 需要与 lastBrokerName 不一样，因为 lastBrokerName 为上次发送失败的 broker，再次发送失败几率也很大。
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }

            // 如果选择的队列所属的 broker = lastBrokerName， 则重新选择！！
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
                + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
