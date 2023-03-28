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

public enum PutMessageStatus {

    // 消息存储成功
    PUT_OK,

    // 刷盘超时，即将消息写入磁盘时超时。
    FLUSH_DISK_TIMEOUT,

    // 同步到从服务器时超时。
    FLUSH_SLAVE_TIMEOUT,

    // 从服务器不可用，同步失败。
    SLAVE_NOT_AVAILABLE,

    // 服务不可用，可能是网络或者其他原因导致。
    SERVICE_NOT_AVAILABLE,

    // 创建映射文件失败。
    CREATE_MAPEDFILE_FAILED,

    // 消息不合法，例如：消息主题长度超过 127 个字符
    MESSAGE_ILLEGAL,

    // 消息属性大小超过限制。
    PROPERTIES_SIZE_EXCEEDED,

    // 操作系统页缓存忙，无法写入磁盘。
    OS_PAGECACHE_BUSY,

    // 未知错误，发送消息失败。
    UNKNOWN_ERROR,
}
