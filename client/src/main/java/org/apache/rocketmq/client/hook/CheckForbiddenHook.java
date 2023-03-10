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

import org.apache.rocketmq.client.exception.MQClientException;


/**
 * CheckForbiddenHook 是 RocketMQ 提供的一个钩子（Hook），在消息发送时可以通过这个钩子来检查消息是否被禁止发送。
 * 如果消息被禁止发送，则可以通过钩子的方式在消息发送前进行拦截并做出相应的处理，例如记录日志、抛出异常等。
 */
public interface CheckForbiddenHook {

    /**
     * 钩子名称
     * @return String
     */
    String hookName();

    /**
     * 具体检查
     *
     * @param context 检查上下文
     * @throws MQClientException 禁止发送则抛出异常
     */
    void checkForbidden(final CheckForbiddenContext context) throws MQClientException;
}
