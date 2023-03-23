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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 主从同步
 * <p>
 * SlaveSynchronize类是用于同步主从 Broker 数据的类，主要用于Master Broker将自己的数据同步给Slave Broker，
 * 从而保证两者数据的一致性。其中包括同步消息、消费队列、索引文件等数据
 */
public class SlaveSynchronize {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    // 相互持有
    private final BrokerController brokerController;
    // 主节点 broker 地址
    private volatile String masterAddr = null;

    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    /**
     * 同步所有数据
     */
    public void syncAll() {
        // 同步主题配置数据
        this.syncTopicConfig();
        // 同步消费进度数据
        this.syncConsumerOffset();
        // 同步延迟队列消费进度数据
        this.syncDelayOffset();
        // 同步订阅组数据
        this.syncSubscriptionGroupConfig();
    }


    /**
     * 用于从主节点同步主题配置信息到从节点
     * 同步对象：TopicConfigManager
     */
    private void syncTopicConfig() {
        // 获取当前从节点保存的主节点地址
        String masterAddrBak = this.masterAddr;
        // 并判断当前 Broker 的地址是否与主节点地址相同。
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                //  从指定 主节点 获取所有主题的配置信息。
                TopicConfigSerializeWrapper topicWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                // 数据版本不同，说明主节点的主题配置信息发生了变化，需要更新从节点的主题配置信息。
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                        .equals(topicWrapper.getDataVersion())) {

                    // 设置当前数据版本
                    this.brokerController.getTopicConfigManager().getDataVersion()
                            .assignNewOne(topicWrapper.getDataVersion());
                    // 清空从节点上的 主题配置信息
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    // 将获取到的主题配置信息添加到从节点的主题配置信息中
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                            .putAll(topicWrapper.getTopicConfigTable());
                    // 并将更新后的主题配置信息保存到磁盘
                    this.brokerController.getTopicConfigManager().persist();

                    // 打印日志
                    log.info("Update slave topic config from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 用于从主节点同步消费者偏移量信息到从节点
     * 同步对象：ConsumerOffsetManager
     */
    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                //  从指定 主节点 查询所有消费者偏移量信息
                ConsumerOffsetSerializeWrapper offsetWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                // 添加到 从节点的 消费者偏移量数据中。
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                        .putAll(offsetWrapper.getOffsetTable());
                // 持久化
                this.brokerController.getConsumerOffsetManager().persist();
                log.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 是从主节点同步延迟消息消费进度到从节点。
     * 同步对象：ScheduleMessageService
     */
    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                //  从指定 主节点 获取所有延迟消息消费进度信息
                String delayOffset = this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {

                    // 获取配置文件名称
                    String fileName =
                            StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                                    .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        // 将这些信息写入从节点的文件中
                        MixAll.string2File(delayOffset, fileName);
                    } catch (IOException e) {
                        log.error("Persist file Exception, {}", fileName, e);
                    }
                }
                log.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }


    /**
     * 该方法是用于同步从节点的订阅组配置信息
     * 同步对象：SubscriptionGroupManager
     */
    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // 从指定 主节点 获取所有订阅组配置信息
                SubscriptionGroupWrapper subscriptionWrapper =
                        this.brokerController.getBrokerOuterAPI()
                                .getAllSubscriptionGroupConfig(masterAddrBak);
                // 数据版本不同，则标识主节点数据发送变化
                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                        .equals(subscriptionWrapper.getDataVersion())) {
                    SubscriptionGroupManager subscriptionGroupManager =
                            this.brokerController.getSubscriptionGroupManager();
                    // 更新版本
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                            subscriptionWrapper.getDataVersion());
                    // 清空数据
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    // 将从节点中的订阅组配置信息更新为主节点中的数据
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                            subscriptionWrapper.getSubscriptionGroupTable());
                    // 持久化
                    subscriptionGroupManager.persist();
                    log.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }
}
