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
package org.apache.rocketmq.broker.subscription;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 订阅组管理器
 */
public class SubscriptionGroupManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    //   保存所有的订阅组配置信息。 key: 订阅组名称 value: 订阅组配置
    private final ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
            new ConcurrentHashMap<String, SubscriptionGroupConfig>(1024);

    // 数据版本，用于在数据变更时进行 CAS 操作。每当订阅组配置信息发生变化时，都会增加版本号。
    private final DataVersion dataVersion = new DataVersion();
    // 相互持有，不序列化
    private transient BrokerController brokerController;

    public SubscriptionGroupManager() {
        this.init();
    }

    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    private void init() {
        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.FILTERSRV_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.SELF_TEST_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.ONS_HTTP_PROXY_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.ONS_HTTP_PROXY_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PULL_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PULL_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PERMISSION_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PERMISSION_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_OWNER_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_OWNER_GROUP, subscriptionGroupConfig);
        }
    }

    /**
     * 更新 订阅组配置
     *
     * @param config 订阅组配置
     */
    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(config.getGroupName(), config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }

        // 更新版本号
        this.dataVersion.nextVersion();
        // 持久化到本地文件
        this.persist();
    }


    /**
     * 用于禁用指定消费组的消费功能
     *
     * @param groupName 订阅组
     */
    public void disableConsume(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.get(groupName);
        if (old != null) {
            old.setConsumeEnable(false);
            // 增加 dataVersion 来通知其他线程该数据已经发生更改
            this.dataVersion.nextVersion();
        }
    }

    /**
     * 查询指定订阅组配置信息
     *
     * @param group 订阅组
     * @return SubscriptionGroupConfig
     */
    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        // 查询
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(group);
        if (null == subscriptionGroupConfig) {
            // 是否允许自动创建 或者 是系统消费者分组
            if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup() || MixAll.isSysConsumerGroup(group)) {
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                SubscriptionGroupConfig preConfig = this.subscriptionGroupTable.putIfAbsent(group, subscriptionGroupConfig);
                if (null == preConfig) {
                    log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
                }
                // 更新版本
                this.dataVersion.nextVersion();
                // 持久化
                this.persist();
            }
        }

        return subscriptionGroupConfig;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getSubscriptionGroupPath(this.brokerController.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                this.dataVersion.assignNewOne(obj.dataVersion);
                this.printLoadDataWhenFirstBoot(obj);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final SubscriptionGroupManager sgm) {
        Iterator<Entry<String, SubscriptionGroupConfig>> it = sgm.getSubscriptionGroupTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionGroupConfig> next = it.next();
            log.info("load exist subscription group, {}", next.getValue().toString());
        }
    }

    public ConcurrentMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    /**
     * 删除 订阅组配置信息
     *
     * @param groupName 订阅组
     */
    public void deleteSubscriptionGroupConfig(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.remove(groupName);
        if (old != null) {
            log.info("delete subscription group OK, subscription group:{}", old);
            // 更新版本号
            this.dataVersion.nextVersion();
            // 更新持久化到本地文件
            this.persist();
        } else {
            log.warn("delete subscription group failed, subscription groupName: {} not exist", groupName);
        }
    }
}
