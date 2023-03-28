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

import java.nio.ByteBuffer;

import org.apache.rocketmq.common.message.MessageExtBatch;

/**
 * RocketMQ 的一个回调接口，用于将序列化后的消息写入到磁盘中。
 * <p>
 * 具体来说，当消息被序列化后，如果要写入磁盘，就需要通过 AppendMessageCallback 提供的回调方法 doAppend 将消息写入到磁盘。
 * 该方法会返回一个 AppendMessageResult 对象，其中包含了写入的字节数和存储时间戳等信息。
 * <p>
 * Write messages callback interface
 */
public interface AppendMessageCallback {

    /**
     * 单条消息
     * 消息序列化后，写入 MapedByteBuffer （即写入 mmap 映射文件，然后等待 flush 刷盘）
     *
     * @param fileFromOffset 表示要写入的映射文件缓冲区的起始偏移量。
     * @param byteBuffer     MappedFile 持有的 MappedByteBuffer 对象调用slice方法 返回的新的 ByteBuffer 对象。
     * @param maxBlank       表示当前映射文件缓冲区中未被使用的最大空间大小。
     * @param msg            表示待写入的消息对象，它是一个 MessageExtBrokerInner 类型的对象。
     * @return AppendMessageResult
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                                 final int maxBlank, final MessageExtBrokerInner msg);

    /**
     * 批量多条消息
     * 消息序列化后，写入 MapedByteBuffer （即写入 mmap 映射文件，然后等待 flush 刷盘）
     *
     * @param fileFromOffset  表示要写入的映射文件缓冲区的起始偏移量。
     * @param byteBuffer      MappedFile 持有的 MappedByteBuffer 对象调用slice方法 返回的新的 ByteBuffer 对象。
     * @param maxBlank        表示当前映射文件缓冲区中未被使用的最大空间大小。
     * @param messageExtBatch 消息对象，批量消息对象。
     * @return AppendMessageResult
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                                 final int maxBlank, final MessageExtBatch messageExtBatch);
}
