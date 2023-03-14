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
package org.apache.rocketmq.broker.offset;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 管理消费进度（consumer offset）的管理器
 * <p>
 * 1、记录消费组（consumer group）的每个消费者在每个队列（message queue）上的消费进度，即该消费者已经消费了该队列中的哪些消息，以及下一条应该消费的消息是哪一条。
 * <p>
 * 2、在 Broker 启动时，从文件系统（通常是文件存储）中加载已有的消费进度信息。
 * <p>
 * 3、在消费者消费消息时，记录消费进度，并定时（每隔一段时间）将其持久化到文件系统中。
 * <p>
 * 4、在 Broker 中定时（每隔一段时间）向 NameServer 汇报各个消费组的消费进度。
 */
public class ConsumerOffsetManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    // 主题分组分隔符
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    // 消费分组 : 队列:偏移量
    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>(512);

    // Broker控制器，相互持有
    private transient BrokerController brokerController;

    public ConsumerOffsetManager() {
    }

    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 扫描清除未订阅主题
     */
    public void scanUnsubscribedTopic() {
        // 遍历
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            // topic@group
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                // 主题
                String topic = arrays[0];
                // 分组
                String group = arrays[1];

                // 订阅数据为空 && 消费者的消费进度是否落后于存储中的消息数据
                if (null == brokerController.getConsumerManager().findSubscriptionData(group, topic) && this.offsetBehindMuchThanData(topic, next.getValue())) {
                    it.remove();
                    log.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }

    /**
     * 检查消费者的消费进度是否落后于存储中的消息数据
     *
     * @param topic 主题
     * @param table 主题对应的 队列消费进度
     * @return boolean
     */
    private boolean offsetBehindMuchThanData(final String topic, ConcurrentMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();

        // 遍历消息队列消费进度 ，循环跳出条件 result = false ！！！
        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            // 获取该消息队列中已存储消息的最小 offset，即该队列中最早的消息的 offset。
            long minOffsetInStore = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, next.getKey());
            // 当前消费者分组 在该队列的 消费偏移量
            long offsetInPersist = next.getValue();
            // 当前消费进度 offsetInPersist 是否落后于存储在该消息队列中的最小 offset minOffsetInStore
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }


    /**
     * 根据消费者组名 group，获取该消费者组所消费的所有主题
     *
     * @param group 消费者分组
     * @return topics
     */
    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<String>();

        // 遍历消费者进度
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            // topic@group
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                // 判断 group 是否相同
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }

        return topics;
    }

    /**
     * 根据 topic，查询消费某个特定主题的所有消费者组
     *
     * @param topic 主题
     * @return groups
     */
    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<String>();

        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                // 判断topic是否相同
                if (topic.equals(arrays[0])) {
                    groups.add(arrays[1]);
                }
            }
        }

        return groups;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId, final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(clientHost, key, queueId, offset);
    }


    /**
     * 提交消费者消费进度
     *
     * @param clientHost 消费者所在的地址和端口信息
     * @param key        消费者组和主题名称的组合，使用 ":" 分割。
     * @param queueId    消息队列ID
     * @param offset     消费者消费进度
     */
    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {
        // 根据 topic@group 查询消费进度
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        // 不存在创建
        if (null == map) {
            map = new ConcurrentHashMap<Integer, Long>(32);
            // 设置消息队列的消费进度
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            // 更新消费进度更新
            Long storeOffset = map.put(queueId, offset);
            // 如果当前消费进度小于之前已经存储在 map 中的消费进度，则会记录一个 WARN 级别的日志，提示消费进度的更新不符合预期。
            if (storeOffset != null && offset < storeOffset) {
                log.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
    }

    /**
     * 查询消费进度的功能
     *
     * @param group   消费者分组
     * @param topic   主题
     * @param queueId 队列ID
     * @return 消费进度
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        // 获取对应的消费进度
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            // 查询指定 queue 的消费进度
            Long offset = map.get(queueId);
            // 不为空返回 offset
            if (offset != null) return offset;
        }

        // 返回-1，表示未查询到消费进度。
        return -1;
    }

    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        // 消费者分组消费进度 配置文件路径； consumerOffset.json
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            // fastjson 反序列化
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }


    /**
     * 查询指定topic在各个消费组中每个队列的最小offset
     *
     * @param topic        要查询的主题（Topic）名称
     * @param filterGroups 需要过滤掉的消费者组（Consumer Group）名称，多个组名称之间使用逗号分隔。
     *                     其中，filterGroups 是一个可选参数。如果不传入该参数或者该参数为空字符串，则表示不需要进行过滤，查询所有消费者组的消费进度。
     *                     如果传入了该参数，则表示需要过滤掉指定的消费者组，只查询不包含在 filterGroups 中的消费者组的消费进度。
     * @return
     */
    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {

        // key：queueId value：offset
        Map<Integer, Long> queueMinOffset = new HashMap<Integer, Long>();
        Set<String> topicGroups = this.offsetTable.keySet();
        // 需要过滤掉的消费者组 不为空则进行过滤
        if (!UtilAll.isBlank(filterGroups)) {
            // 逗号分割，遍历 filter group
            for (String group : filterGroups.split(",")) {
                Iterator<String> it = topicGroups.iterator();
                while (it.hasNext()) {
                    // 判断 group 是否相同，相同则需要过滤掉
                    if (group.equals(it.next().split(TOPIC_GROUP_SEPARATOR)[1])) {
                        // TODO：直接删除  offsetTable 中的数据 ？？？
                        it.remove();
                    }
                }
            }
        }

        // 遍历消费者进度
        for (Map.Entry<String, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable.entrySet()) {
            String topicGroup = offSetEntry.getKey();
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            // 判断是否为 指定要查询的 topic  ！！！
            if (topic.equals(topicGroupArr[0])) {
                // 遍历消息队列的消费进度
                for (Entry<Integer, Long> entry : offSetEntry.getValue().entrySet()) {
                    // 获取该消息队列中已存储消息的最小 offset，即该队列中最早的消息的 offset。
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, entry.getKey());
                    // 当前消费进度 >= minOffset 才算是有效进度
                    if (entry.getValue() >= minOffset) {
                        // 获取消费进度
                        Long offset = queueMinOffset.get(entry.getKey());
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), Math.min(Long.MAX_VALUE, entry.getValue()));
                        } else {
                            // 添加至结果集合
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
                    }
                }
            }

        }
        return queueMinOffset;
    }

    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<Integer, Long>(offsets));
        }
    }

}
