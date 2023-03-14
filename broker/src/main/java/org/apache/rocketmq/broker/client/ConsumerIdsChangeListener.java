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


/**
 * 消费者ID变更监听器
 * 接口定义了消费者 ID 变化事件的监听器接口，主要用于通知消费者 ID 变化的事件。
 */
public interface ConsumerIdsChangeListener {

    /**
     * 通知处理
     *
     * @param event 事件
     * @param group 消费者分组
     * @param args
     */
    void handle(ConsumerGroupEvent event, String group, Object... args);
}
