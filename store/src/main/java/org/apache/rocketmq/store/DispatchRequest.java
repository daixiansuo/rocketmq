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
package org.apache.rocketmq.store;

import java.util.Map;

public class DispatchRequest {

    // 主题
    private final String topic;
    // 队列ID
    private final int queueId;
    // 消息在 CommitLog 中的物理偏移量。
    private final long commitLogOffset;
    // 消息大小
    private int msgSize;
    // 消息的 TagCode，用于查询消息。
    private final long tagsCode;
    // 消息存储时间戳。
    private final long storeTimestamp;
    // 消息在 ConsumeQueue 中的偏移量。
    private final long consumeQueueOffset;
    // 消息关键词
    private final String keys;
    // 是否成功发送消息。
    private final boolean success;
    // 消息的唯一键。
    private final String uniqKey;
    // 消息的系统标识
    private final int sysFlag;

    // 如果是事务消息，则该字段表示消息的事务偏移量；否则为 0。
    private final long preparedTransactionOffset;
    // 消息的属性集合，用于消息过滤。
    private final Map<String, String> propertiesMap;
    // 消息的 BitMap，用于查询消息。
    private byte[] bitMap;

    /**
     * 表示消息在缓冲区中的大小，初始值为-1。
     * <p>
     * 在某些情况下，消息可能被封装在其他对象中，如TransactionMQProducer发送事务消息时，
     * 需要将原始消息和事务相关信息封装在一个TransactionMQProducerImpl对象中。
     * 在这种情况下，消息的实际大小可能小于封装后的对象的大小。因此，bufferSize记录的是封装后的对象大小，而msgSize记录的是消息实际大小，两者可能不相等
     */
    private int bufferSize = -1;//the buffer size maybe larger than the msg size if the message is wrapped by something

    public DispatchRequest(
            final String topic,
            final int queueId,
            final long commitLogOffset,
            final int msgSize,
            final long tagsCode,
            final long storeTimestamp,
            final long consumeQueueOffset,
            final String keys,
            final String uniqKey,
            final int sysFlag,
            final long preparedTransactionOffset,
            final Map<String, String> propertiesMap
    ) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;
        this.uniqKey = uniqKey;

        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(int size) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }

    public String getKeys() {
        return keys;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public byte[] getBitMap() {
        return bitMap;
    }

    public void setBitMap(byte[] bitMap) {
        this.bitMap = bitMap;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
