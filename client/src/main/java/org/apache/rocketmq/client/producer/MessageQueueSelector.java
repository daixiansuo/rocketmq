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
package org.apache.rocketmq.client.producer;

import java.util.List;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息队列选择器
 * <p>
 * 用于在消息发送时选择要发送到哪个消息队列
 *
 * 默认有三个实现：
 * byHash 哈希
 * byRandom 随机
 * byMachineRom -- > 这个没实现
 */
public interface MessageQueueSelector {

    /**
     * 选择消息队列
     *
     * @param mqs 队列列表
     * @param msg 消息
     * @param arg 附近参数
     * @return MessageQueue
     */
    MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
