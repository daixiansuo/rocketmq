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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class ProducerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // 锁等待超时时间，单位为毫秒。
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    // 客户端网络连接过期时间，单位为毫秒。
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    // 获取可用连接时的重试次数。
    private static final int GET_AVALIABLE_CHANNEL_RETRY_COUNT = 3;

    // 用于控制分组与网络连接的并发访问的锁。
    private final Lock groupChannelLock = new ReentrantLock();

    // 维护了一个分组名称到连接信息的映射表，每个分组对应一个连接信息的映射表，其中连接信息为 ClientChannelInfo 对象。
    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable = new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
    // 维护了一个客户端 ID 到连接信息的映射表，其中连接信息为 Channel 对象。
    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap<>();

    // 用于生成 Channel ID 的计数器
    private PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    public ProducerManager() {
    }

    /**
     * 获取 groupChannelTable 副本数据
     *
     * @return HashMap<String, HashMap < Channel, ClientChannelInfo>>
     */
    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable = new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
        try {
            // 获取锁
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    // 遍历所有 生产者分组信息
                    Iterator<Map.Entry<String, HashMap<Channel, ClientChannelInfo>>> iter = groupChannelTable.entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry = iter.next();
                        String key = entry.getKey();
                        HashMap<Channel, ClientChannelInfo> val = entry.getValue();
                        HashMap<Channel, ClientChannelInfo> tmp = new HashMap<Channel, ClientChannelInfo>();
                        tmp.putAll(val);
                        newGroupChannelTable.put(key, tmp);
                    }
                } finally {
                    groupChannelLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return newGroupChannelTable;
    }

    public void scanNotActiveChannel() {
        try {
            // 获取锁
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    // 遍历所有生产者分组-客户端通道信息
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable.entrySet()) {
                        final String group = entry.getKey();
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        // 遍历对应的 客户端通道信息
                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            final ClientChannelInfo info = item.getValue();
                            // 计算通道最后一次更新时间与当前时间的差异
                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            // 如果超过了阈值，则认为该通道已经过期，并将其关闭。然后它将从通道信息表中删除该通道。
                            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                                // 删除客户端信息
                                it.remove();
                                // 删除channel
                                clientChannelTable.remove(info.getClientId());
                                log.warn("SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}", RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                // 关闭 channel连接
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    /**
     * 生产者连接关闭事件
     *
     * @param remoteAddr 客户端地址
     * @param channel    通道
     */
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
                if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        // 遍历每一个生产者分组信息
                        for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable.entrySet()) {
                            // 生产者分组
                            final String group = entry.getKey();
                            // 对应的 生产者通道信息集合
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable = entry.getValue();

                            // 删除需要关闭的 channel
                            final ClientChannelInfo clientChannelInfo = clientChannelInfoTable.remove(channel);
                            // 删除成功返回的旧数据不为空，则删除 客户端通道集合 中的数据。
                            if (clientChannelInfo != null) {
                                // 删除对应的数据
                                clientChannelTable.remove(clientChannelInfo.getClientId());
                                log.info("NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}", clientChannelInfo.toString(), remoteAddr, group);
                            }

                        }
                    } finally {
                        this.groupChannelLock.unlock();
                    }
                } else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }


    /**
     * 注册生产者
     *
     * @param group             生产者分组
     * @param clientChannelInfo 客户端通道信息
     */
    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            ClientChannelInfo clientChannelInfoFound = null;

            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {

                    // 查询 生产者分组 对应的 客户端通道信息集合
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null == channelTable) {
                        // 为空，创建个空Map
                        channelTable = new HashMap<>();
                        // 先将空Map 添加进去
                        this.groupChannelTable.put(group, channelTable);
                    }

                    // 根据 channel 查询对应的 客户端通道信息
                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
                    if (null == clientChannelInfoFound) {
                        // 为空则，注册到本地缓存中
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        // 同时注册到 客户端通道表
                        clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
                        log.info("new producer connected, group: {} channel: {}", group, clientChannelInfo.toString());
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }

                // 如果不为空，则需要修改 该通道上一次更新的时间戳
                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            } else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    /**
     * 注销生产者
     *
     * @param group             生产者分组
     * @param clientChannelInfo 客户端通道信息
     */
    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    // 查询 生产者分组 对应的 客户端通道信息集合
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null != channelTable && !channelTable.isEmpty()) {

                        // 删除对应的 客户端信息
                        ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        // 删除对应的 通道信息
                        clientChannelTable.remove(clientChannelInfo.getClientId());
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group, clientChannelInfo.toString());
                        }

                        // 删除之后，如果为空，则删除对应的 生产者分组数据
                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }


    /**
     * 获取指定 生产者分组 下可用生产者实例的 channel对象。
     *
     * @param groupId 生产者分组
     * @return Channel
     */
    public Channel getAvaliableChannel(String groupId) {

        // 根据 生产者分组 查询对应的 客户端通道信息集合； 即一个生产者分组 对应多个 生产者实例。
        HashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        List<Channel> channelList = new ArrayList<Channel>();
        if (channelClientChannelInfoHashMap != null) {

            // 遍历所有生产者通道信息
            for (Channel channel : channelClientChannelInfoHashMap.keySet()) {
                channelList.add(channel);
            }

            // 如果 channelList 长度为零，则返回 null，且打印警告日志。
            int size = channelList.size();
            if (0 == size) {
                log.warn("Channel list is empty. groupId={}", groupId);
                return null;
            }

            // 取模
            int index = positiveAtomicCounter.incrementAndGet() % size;
            Channel channel = channelList.get(index);
            int count = 0;
            boolean isOk = channel.isActive() && channel.isWritable();

            // 最大循环次数 =  获取可用连接时的重试次数
            while (count++ < GET_AVALIABLE_CHANNEL_RETRY_COUNT) {
                // 获取的channel状态可用，则直接返回。
                if (isOk) {
                    return channel;
                }
                // 不可用，递增&取模
                index = (++index) % size;
                channel = channelList.get(index);
                isOk = channel.isActive() && channel.isWritable();
            }
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }
        return null;
    }


    /**
     * 根据 客户端ID 查询对应的Channel
     *
     * @param clientId 客户端ID
     * @return Channel
     */
    public Channel findChannel(String clientId) {
        return clientChannelTable.get(clientId);
    }
}
