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
package org.apache.rocketmq.broker.client.rebalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageQueue;


/**
 * RebalanceLockManager是RocketMQ中用于实现消费者负载均衡的重要组件，主要作用是管理和分配消费者组与消息队列之间的锁，
 * 以确保同一时刻只有一个消费者能够消费同一个消息队列。从而保证消息的有序性和消费的负载均衡性。
 */
public class RebalanceLockManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);

    /**
     * 表示RebalanceLock的最大生存时间，单位是毫秒。如果一个RebalanceLock在超过这个时间之后还未被释放，那么它将被认为是过期的，从而被强制释放。
     */
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
            "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));

    // 用于保护mqLockTable并发更新的锁
    private final Lock lock = new ReentrantLock();

    /**
     * 用于保存每个Consumer Group及其所持有的MessageQueue的锁对象，其中锁对象是一个包含了锁状态和锁持有者等信息的数据结构。
     * 通过这个表，Broker可以判断某个Consumer Group是否已经持有了某个MessageQueue的锁，从而防止其它Consumer Group对同一个MessageQueue进行消费。
     * 同时，Broker可以在某个Consumer Group因为各种原因不能完成Rebalance操作时，强制释放其持有的锁，从而避免出现死锁的情况。
     */
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
            new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);


    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                group,
                                clientId,
                                mq);
                    }

                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                                "tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                        return true;
                    }

                    log.warn(
                            "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }


    /**
     * 判断指定的 消费者 是否持有指定 queue 的锁 ！！
     *
     * @param group    消费者分组
     * @param mq       队列
     * @param clientId 消费者客户端ID
     * @return boolean
     */
    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                // 判断当前锁是否被特定的消费者持有 （如果 clientId 都不相同，那就肯定没有只有该 queue 的锁）
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    // 如果当前消费者持有该 queue 的锁，则更新最后一次更新锁时间
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }

    /**
     * 指定消费者 尝试获取 指定的一批 queue 的锁。或者说尝试锁定一批 queue。
     *
     * @param group    消费者分组
     * @param mqs      队列列表
     * @param clientId 消费者客户端ID
     * @return 返回已经持有锁的队列列表
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
                                          final String clientId) {

        // 存储已经持有锁的队列
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());
        // 存储没有持有锁的队列
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        // 遍历判断
        for (MessageQueue mq : mqs) {
            // 判断当前锁是否被指定的消费者持有
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
            } else {
                notLockedMqs.add(mq);
            }
        }

        // 遍历没有持有锁的队列，逐个判断是否能够加锁
        if (!notLockedMqs.isEmpty()) {
            try {
                // 加锁，可中断
                this.lock.lockInterruptibly();
                try {

                    // 获取或创建 指定消费组 的锁定记录
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    // 逐个处理没有持有锁的队列
                    for (MessageQueue mq : notLockedMqs) {

                        // 如果该队列没有被锁定过，则创建新的锁定记录，并将其设置为当前消费者持有的锁。
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info(
                                    "tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                    group,
                                    clientId,
                                    mq);
                        }

                        // 如果该队列已经被锁定，并且锁的持有者是当前消费者，则更新锁的最后更新时间，并将该 MessageQueue 添加到已经持有锁的队列中。
                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            // 跳过
                            continue;
                        }


                        // 如果该队列已经被锁定，但是锁的持有者不是当前消费者
                        String oldClientId = lockEntry.getClientId();

                        // 如果锁已经过期，则将锁的持有者设置为当前消费者，并将其设置为当前消费者持有的锁，同时将该 MessageQueue 添加到已经持有锁的队列中。
                        // 如果没过期，则忽略处理
                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                    "tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                    group,
                                    oldClientId,
                                    clientId,
                                    mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn(
                                "tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }

        return lockedMqs;
    }

    /**
     * 释放一批消息队列的锁
     *
     * @param group    消费者分组
     * @param mqs      消息队列列表
     * @param clientId 消费者客户端ID
     */
    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                // 根据 消费者分组 获取对应的 队列锁记录。不存在打印warn日志
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {

                    // 遍历需要释放锁的队列列表
                    for (MessageQueue mq : mqs) {
                        // 根据 queue 获取锁记录
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                            // 判断锁的持有者 是不是当前消费者，如果是，则删除锁。
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("unlockBatch, Group: {} {} {}",
                                        group,
                                        mq,
                                        clientId);
                            } else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",
                                        lockEntry.getClientId(),
                                        group,
                                        mq,
                                        clientId);
                            }
                        } else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",
                                    group,
                                    mq,
                                    clientId);
                        }
                    }
                } else {
                    log.warn("unlockBatch, group not exist, Group: {} {}",
                            group,
                            clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }


    /**
     * LockEntry 是 RebalanceLockManager 的内部类，用于描述某个消费组在某个消息队列上的锁信息。
     * 其中 clientId 表示当前持有锁的消费者的 客户端ID = ClientIP + @ + InstanceName + [ @ + unitName]，
     * lastUpdateTimestamp 表示最后一次更新锁时间的时间戳。
     * isLocked 用于判断当前锁是否被特定的消费者持有，isExpired 用于判断当前锁是否已过期。
     */
    static class LockEntry {
        private String clientId;
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        /**
         * 判断当前锁是否被特定的消费者持有 （锁没有过期）
         *
         * @param clientId 客户端ID
         * @return boolean
         */
        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        public boolean isExpired() {
            boolean expired =
                    (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
