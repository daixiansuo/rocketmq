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
package org.apache.rocketmq.store.index;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IndexHeader {

    // IndexFile 的头部，共占 40 个字节
    public static final int INDEX_HEADER_SIZE = 40;
    // 各个字段的开始索引位置
    private static int beginTimestampIndex = 0;
    private static int endTimestampIndex = 8;
    private static int beginPhyoffsetIndex = 16;
    private static int endPhyoffsetIndex = 24;
    private static int hashSlotcountIndex = 32;
    private static int indexCountIndex = 36;

    // 存储头部信息的 缓冲区， 构造方法时赋值。
    private final ByteBuffer byteBuffer;

    // 该 IndexFile 文件中包含消息的最小存储时间。
    private AtomicLong beginTimestamp = new AtomicLong(0);
    // 该 IndexFile 文件中包含消息的最大存储时间。
    private AtomicLong endTimestamp = new AtomicLong(0);
    // 该 IndexFile 文件中包含消息的最小 CommitLog 文件偏移量。
    private AtomicLong beginPhyOffset = new AtomicLong(0);
    // 该 IndexFile 文件中包含消息的最大 CommitLog 文件偏移量
    private AtomicLong endPhyOffset = new AtomicLong(0);
    // 该 IndexFile 文件中包含的 hashSlot 的总数。
    private AtomicInteger hashSlotCount = new AtomicInteger(0);
    // 该 IndexFile 文件中已使用的 Index 条目个数。
    private AtomicInteger indexCount = new AtomicInteger(1);



    public IndexHeader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    /**
     * 加载数据
     */
    public void load() {

        // 从缓冲区读取数据

        // 开始、结束 （时间 和 CommitLog 文件偏移量）
        this.beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
        this.endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
        this.beginPhyOffset.set(byteBuffer.getLong(beginPhyoffsetIndex));
        this.endPhyOffset.set(byteBuffer.getLong(endPhyoffsetIndex));

        // hashSlot总数、已使用的 Index 条目个数。
        this.hashSlotCount.set(byteBuffer.getInt(hashSlotcountIndex));
        this.indexCount.set(byteBuffer.getInt(indexCountIndex));

        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1);
        }
    }

    /**
     * 更新缓冲区数据
     */
    public void updateByteBuffer() {
        this.byteBuffer.putLong(beginTimestampIndex, this.beginTimestamp.get());
        this.byteBuffer.putLong(endTimestampIndex, this.endTimestamp.get());
        this.byteBuffer.putLong(beginPhyoffsetIndex, this.beginPhyOffset.get());
        this.byteBuffer.putLong(endPhyoffsetIndex, this.endPhyOffset.get());
        this.byteBuffer.putInt(hashSlotcountIndex, this.hashSlotCount.get());
        this.byteBuffer.putInt(indexCountIndex, this.indexCount.get());
    }

    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
    }

    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(endTimestampIndex, endTimestamp);
    }

    public long getBeginPhyOffset() {
        return beginPhyOffset.get();
    }

    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(beginPhyoffsetIndex, beginPhyOffset);
    }

    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }

    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(endPhyoffsetIndex, endPhyOffset);
    }

    public AtomicInteger getHashSlotCount() {
        return hashSlotCount;
    }

    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(hashSlotcountIndex, value);
    }

    public int getIndexCount() {
        return indexCount.get();
    }

    /**
     * 增加 index条目 使用数量
     */
    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(indexCountIndex, value);
    }
}
