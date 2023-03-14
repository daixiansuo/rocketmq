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
package org.apache.rocketmq.common.consumer;

/**
 * 消费位置，从哪里开始消费
 */
public enum ConsumeFromWhere {

    // 从上次消费的位置开始消费，若无消费记录，则从最新的消息开始消费。
    CONSUME_FROM_LAST_OFFSET,

    // 启动时从队列最小的位置开始消费，之后从上次消费的位置开始消费，若无消费记录，则从最新的消息开始消费。已经废弃，不建议使用
    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,

    @Deprecated
    CONSUME_FROM_MIN_OFFSET, // 从队列最小的位置开始消费
    @Deprecated
    CONSUME_FROM_MAX_OFFSET, // 从队列最大的位置开始消费

    // 从该队列最开始的位置开始消费
    CONSUME_FROM_FIRST_OFFSET,
    // 从指定时间点开始消费，时间戳通过 DefaultMQPushConsumer#setConsumeTimestamp() 设置
    CONSUME_FROM_TIMESTAMP,
}
