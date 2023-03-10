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
package org.apache.rocketmq.client.hook;


/**
 * 消息发送钩子函数
 * <p>
 * 可以在消息发送前、发送后以及发送异常时执行一些自定义逻辑，例如日志记录、消息统计等。
 */
public interface SendMessageHook {

    // 钩子名称
    String hookName();


    /**
     * 消息发送前执行
     *
     * @param context 上下文对象
     */
    void sendMessageBefore(final SendMessageContext context);

    /**
     * 消息发送后执行
     *
     * @param context 上下文对象
     */
    void sendMessageAfter(final SendMessageContext context);
}
