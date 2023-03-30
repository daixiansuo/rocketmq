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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * Index 文件组成
 * <p>
 * ｜-------------------｜------------------------｜-----------------------｜
 * ｜    Index Header   ｜      500万个哈希槽       ｜  2000万个Index条目      ｜
 * ｜-------------------｜------------------------｜-----------------------｜
 * <p>
 * Index Header(40): beginTimestamp(8) + endTimestamp(8) + beginPhyOffset(8) + endPhyOffset(8) + hashSlotCount(4) + indexCount(4)
 * 哈希槽: 每个哈希槽占用 4字节。哈希槽 存储的是落在该哈希槽的哈希码 最新的Index条目的索引 ！！！
 * Index条目: 每个Index条目占用20字节，包含 hashCode + phyoffset + timedif + preIndexNo
 */

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 哈希槽的大小，默认值为 4 个字节
    private static int hashSlotSize = 4;
    // 索引项的大小，默认值为 20 个字节，其中包含 哈希值(4) 、物理偏移量(8)，该消息存储时间与第一条消息的时间戳的差值(4)、该条目的前一条记录的Index索引(4)。
    private static int indexSize = 20;
    // 表示一个无效的索引项的值，即哈希槽或索引项的值为空。
    private static int invalidIndex = 0;

    // 哈希槽数量，
    private final int hashSlotNum;
    // 索引项数量
    private final int indexNum;
    // 索引文件对应的 MappedFile 实例，它封装了索引文件的相关操作。
    private final MappedFile mappedFile;
    // 索引文件对应的文件通道，用于读写索引文件。
    private final FileChannel fileChannel;
    // 索引文件对应的 MappedByteBuffer 实例，用于操作索引文件。
    private final MappedByteBuffer mappedByteBuffer;
    // 索引文件头部信息，包含哈希槽数量、索引项数量以及最后一条消息在 CommitLog 文件中的物理偏移量和时间戳。
    private final IndexHeader indexHeader;


    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
                     final long endPhyOffset, final long endTimestamp) throws IOException {

        // 计算 IndexFile 文件大小 = Index 文件头 + 哈希槽 + Index 条目
        int fileTotalSize =
                IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);

        // 创建映射文件
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        // 文件通道
        this.fileChannel = this.mappedFile.getFileChannel();
        // 映射缓冲区
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        // 哈希槽数量
        this.hashSlotNum = hashSlotNum;
        // Index条目数量
        this.indexNum = indexNum;

        // 初始化文件头
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        // 如果大于零，则更新文件头信息
        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        // 如果大于零，则更新文件头信息
        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    /**
     * 刷盘
     */
    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    /**
     * 是否写满
     *
     * @return boolean
     */
    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }


    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 将索引信息写入到索引文件中。
     *
     * @param key            topic + # + key
     * @param phyOffset      消息的物理偏移量。
     * @param storeTimestamp 消息存储时间
     * @return boolean
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {

        // 判断当前索引数量 是否 已经超过了预设值
        if (this.indexHeader.getIndexCount() < this.indexNum) {

            // 计算 key 的哈希值
            int keyHash = indexKeyHashMethod(key);
            // 根据 keyHash 对哈希槽数量取余，定位到哈希码对应的哈希槽下标
            int slotPos = keyHash % this.hashSlotNum;
            // keyHash 对应的哈希槽的物理地址 = IndexHeader（40字节）+ 下标乘以每个哈希槽的大小（4字节）
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {

                // TODO：如果哈希槽中存储的值为0或大于当前Index文件最大条目数或小于-1，即不存在哈希冲突！！！！
                // 读取哈希槽中存储的数据，如果哈希槽存储的数据小于0 或 大于当前Index文件中的索引条目，则将slotValue设置为0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 计算待存储消息的时间戳与第一条消息时间戳的差值，并转换成秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                timeDiff = timeDiff / 1000;

                // 无效数据检测
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 将条目信息存储在Index文件中
                // 计算新添加条目的起始物理偏移量：头部字节长度 + 哈希槽数量×单个哈希槽大小（4个字节）+ 当前Index条目个数×单个Index条目大小（20个字节）。
                int absIndexPos =
                        IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + this.indexHeader.getIndexCount() * indexSize;

                // ============================== 添加 Index 条目==========================================
                // 哈希码
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 消息物理偏移量（commitLogOffset）
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 消息存储时间戳与Index文件时间戳 时间差，单位/秒
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // TODO：如果当前哈希槽的值不为0，说明之前有数据，即存在哈希冲突。
                // 该条目的前一条记录的Index索引，当出现哈希冲突时，构建链表结构。
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // ============================== 添加 Index 哈希槽 ==========================================

                // 哈希槽存储的是落在该哈希槽的哈希码最新的Index索引（覆盖原先哈希槽的值）。
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // ============================== 更新 Index 文件头 ==========================================

                // 如果当前文件只包含一个条目，则记录 beginPhyOffset、beginTimestamp。
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 更新 哈希槽数量
                this.indexHeader.incHashSlotCount();
                // 更新 index条目 数量
                this.indexHeader.incIndexCount();
                // 更新 最后时间、最大物理偏移量
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                    + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 作用是对消息索引的关键字（key）进行哈希计算
     *
     * @param key topic + # + key
     * @return 哈希值
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }


    /**
     * 方法的作用是判断给定的时间区间(begin, end)是否 与当前IndexFile所维护的时间区间有交集
     *
     * @param begin 开始时间
     * @param end   结束时间
     * @return boolean
     */
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }


    /**
     * 根据给定的 key 和 时间范围，从索引文件中查询对应的物理偏移量
     *
     * @param phyOffsets 用于存储查询结果的物理偏移量列表
     * @param key        用于查询的关键字
     * @param maxNum     最大返回的结果数量
     * @param begin      查询的开始时间
     * @param end        结束时间
     * @param lock       表示是否需要对索引文件进行加锁。
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end, boolean lock) {

        // 判断当前的 mappedFile 对象是否被其他线程持有，如果被持有则返回，否则持有该mappedFile对象。
        if (this.mappedFile.hold()) {

            // 计算 key 的哈希翼
            int keyHash = indexKeyHashMethod(key);
            // 根据 keyHash 对哈希槽数量取余，定位到哈希码对应的哈希槽下标
            int slotPos = keyHash % this.hashSlotNum;
            // keyHash 对应的哈希槽的物理地址 = IndexHeader（40字节）+ 下标乘以每个哈希槽的大小（4字节）
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 读取哈希槽中存储的数据，哈希槽存储的是落在该哈希槽的哈希码最新的Index索引
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                // 判断该hash槽是否有效，如果无效则直接返回，否则继续进行查询。
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) {
                } else {

                    // 从当前索引项开始，依次遍历索引链表，直到找到符合条件的索引项或者达到最大返回结果数量。
                    for (int nextIndexToRead = slotValue; ; ) {

                        // 判断当前 查询的数据 是否满足需要查询的条目，满足则跳出循环
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        // 根据 哈希槽中存储的 index 条目下标，计算当前index条目的起始物理偏移量。
                        int absIndexPos =
                                IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                        + nextIndexToRead * indexSize;

                        // 读取 key的哈希值
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        // 读取 消息物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        // 读取 当前消息与indexFile文件的开始时间的 时间差，秒数。
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        // 读取上一个的 index条目的索引。（存储hash冲突时 不为0）
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        // 单位转换，秒 转 毫秒。
                        if (timeDiff < 0) {
                            break;
                        }
                        timeDiff *= 1000L;

                        // 计算当前的消息存储时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        // 判断当前消息的时间 是否在查询范围之内 ！！
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        // 如果 哈希值相同 且 时间范围匹配，则添加到结果集当中。
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        // 以下几种情况跳出循环
                        if (prevIndexRead <= invalidIndex
                                || prevIndexRead > this.indexHeader.getIndexCount()
                                || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        // 更新index索引，继续循环读取。（这种情况是存储哈希冲突时才会有）
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
