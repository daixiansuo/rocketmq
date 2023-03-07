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
package org.apache.rocketmq.common.protocol;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;

public class NamespaceUtil {

    // 分隔符
    public static final char NAMESPACE_SEPARATOR = '%';
    // 空字符串
    public static final String STRING_BLANK = "";

    // 重试前缀 长度 -- %RETRY% = 7
    public static final int RETRY_PREFIX_LENGTH = MixAll.RETRY_GROUP_TOPIC_PREFIX.length();
    // 死信队列前缀 长度  -- %DLQ% = 5
    public static final int DLQ_PREFIX_LENGTH = MixAll.DLQ_GROUP_TOPIC_PREFIX.length();

    /**
     * TODO: 从 "资源" 中删除 namespace, 资源(topic/groupId) 是指包含 namespace 的字符串。
     * <p>
     * Unpack namespace from resource, just like:
     * (1) MQ_INST_XX%Topic_XXX --> Topic_XXX
     * (2) %RETRY%MQ_INST_XX%GID_XXX --> %RETRY%GID_XXX
     *
     * @param resourceWithNamespace 资源(topic/groupId) 是指包含 namespace 的字符串。
     * @return 去除掉 namespace 的资源(topic/groupId)。
     */
    public static String withoutNamespace(String resourceWithNamespace) {

        // 为空 或者 为系统资源 直接返回 resourceWithNamespace
        if (StringUtils.isEmpty(resourceWithNamespace) || isSystemResource(resourceWithNamespace)) {
            return resourceWithNamespace;
        }

        StringBuffer strBuffer = new StringBuffer();

        // 如果是 重试topic，拼接重试前缀
        if (isRetryTopic(resourceWithNamespace)) {
            strBuffer.append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        }

        // 如果是 死信topic，拼接死信前缀
        if (isDLQTopic(resourceWithNamespace)) {
            strBuffer.append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
        }

        // 去除 资源(topic、groupId)名称中 包含的 重试、死信前缀。
        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithNamespace);
        // 查询 namespace分隔符 索引
        int index = resourceWithoutRetryAndDLQ.indexOf(NAMESPACE_SEPARATOR);
        // 若果 index > 0 说明包含 namespace
        if (index > 0) {
            // 去除 namespace、保留资源名称(topic/groupId)
            String resourceWithoutNamespace = resourceWithoutRetryAndDLQ.substring(index + 1);
            // 拼接上 资源名称，返回最终去除namespace的结果。
            return strBuffer.append(resourceWithoutNamespace).toString();
        }

        return resourceWithNamespace;
    }

    /**
     * If resource contains the namespace, unpack namespace from resource, just like:
     * (1) (MQ_INST_XX1%Topic_XXX1, MQ_INST_XX1) --> Topic_XXX1
     * (2) (MQ_INST_XX2%Topic_XXX2, NULL) --> MQ_INST_XX2%Topic_XXX2
     * (3) (%RETRY%MQ_INST_XX1%GID_XXX1, MQ_INST_XX1) --> %RETRY%GID_XXX1
     * (4) (%RETRY%MQ_INST_XX2%GID_XXX2, MQ_INST_XX3) --> %RETRY%MQ_INST_XX2%GID_XXX2
     *
     * @param resourceWithNamespace 资源(topic/groupId) 是指包含 namespace 的字符串。
     * @param namespace             指定需要去除的 namespace。
     * @return 去除掉 namespace 的资源(topic/groupId)。
     */
    public static String withoutNamespace(String resourceWithNamespace, String namespace) {

        // 为空 直接返回。
        if (StringUtils.isEmpty(resourceWithNamespace) || StringUtils.isEmpty(namespace)) {
            return resourceWithNamespace;
        }

        // 去除 资源(topic、groupId)名称中 包含的 重试、死信前缀。
        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithNamespace);
        // 判断当前 资源(topic、groupId)名称 是否以指定的 namespace 开头！！
        if (resourceWithoutRetryAndDLQ.startsWith(namespace + NAMESPACE_SEPARATOR)) {
            // 去除 namespace
            return withoutNamespace(resourceWithNamespace);
        }

        return resourceWithNamespace;
    }


    /**
     * TODO: 从 "资源" 中添加上 namespace, 资源(topic/groupId) 是指对应资源的名称字符串。
     * 注意：系统资源(topic/groupId)名称 不需要处理
     *
     * @param namespace                命名空间
     * @param resourceWithOutNamespace 没有包含 namespace 的资源(topic/groupId)
     * @return 包含 namespace 的资源(topic/groupId)
     */
    public static String wrapNamespace(String namespace, String resourceWithOutNamespace) {

        // 为空直接返回
        if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resourceWithOutNamespace)) {
            return resourceWithOutNamespace;
        }

        // 判断 资源(topic/groupId) 是否为系统资源 或者 是否已经包含 namespace
        if (isSystemResource(resourceWithOutNamespace) || isAlreadyWithNamespace(resourceWithOutNamespace, namespace)) {
            return resourceWithOutNamespace;
        }

        // 去除 资源(topic、groupId)名称中 包含的 重试、死信前缀。
        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithOutNamespace);
        StringBuffer strBuffer = new StringBuffer();

        // TODO: 注意此处判断的变量是 resourceWithOutNamespace，而不是 resourceWithoutRetryAndDLQ ！！

        // 如果是 重试topic，拼接重试前缀
        if (isRetryTopic(resourceWithOutNamespace)) {
            strBuffer.append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        }

        // 如果是 死信topic，拼接死信前缀
        if (isDLQTopic(resourceWithOutNamespace)) {
            strBuffer.append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
        }

        // TODO: 最后格式 = [%RETRY%/%DLQ%] + [namespace] + % + 资源名称(topic/groupId)
        // 最后拼接 namespace + % + (topic/groupId)
        return strBuffer.append(namespace).append(NAMESPACE_SEPARATOR).append(resourceWithoutRetryAndDLQ).toString();

    }

    /**
     * 是否已经包含 namespace
     *
     * @param resource  资源名称
     * @param namespace 命名空间
     * @return boolean
     */
    public static boolean isAlreadyWithNamespace(String resource, String namespace) {

        // 为空 或者 为系统资源 直接返回false
        if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return false;
        }

        // 将 resource 中的 重试、死信前缀去除。
        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resource);

        // 判断当前 resource 是否以 namespace% 开头。如果是，则表示当前 resource 已经包含的 namespace。
        return resourceWithoutRetryAndDLQ.startsWith(namespace + NAMESPACE_SEPARATOR);
    }

    public static String wrapNamespaceAndRetry(String namespace, String consumerGroup) {
        if (StringUtils.isEmpty(consumerGroup)) {
            return null;
        }

        return new StringBuffer()
                .append(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                .append(wrapNamespace(namespace, consumerGroup))
                .toString();
    }

    public static String getNamespaceFromResource(String resource) {
        if (StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return STRING_BLANK;
        }
        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resource);
        int index = resourceWithoutRetryAndDLQ.indexOf(NAMESPACE_SEPARATOR);

        return index > 0 ? resourceWithoutRetryAndDLQ.substring(0, index) : STRING_BLANK;
    }


    /**
     * 去除 资源(topic、groupId)名称中 包含的 重试、死信前缀。
     *
     * @param originalResource 资源(topic/groupId)名称
     * @return 资源(topic / groupId)
     */
    private static String withOutRetryAndDLQ(String originalResource) {
        if (StringUtils.isEmpty(originalResource)) {
            return STRING_BLANK;
        }

        // 如果是 重试主题，去除重试前缀，直接返回。
        if (isRetryTopic(originalResource)) {
            return originalResource.substring(RETRY_PREFIX_LENGTH);
        }

        // 注意：重试与死信 两者不会同时存在！
        if (isDLQTopic(originalResource)) {
            return originalResource.substring(DLQ_PREFIX_LENGTH);
        }

        return originalResource;
    }


    /**
     * 是否为 系统资源(topic\consumerGroup)
     *
     * @param resource 资源名称
     * @return boolean
     */
    private static boolean isSystemResource(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        // TODO：systemTopic 是指以 rmq_sys_ 开头的 topic； systemConsumerGroup 是指 CID_RMQ_SYS_ 开头的 consumerGroup。
        if (MixAll.isSystemTopic(resource) || MixAll.isSysConsumerGroup(resource)) {
            return true;
        }

        // 默认创建的TOPIC ： TBW102
        return MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC.equals(resource);
    }


    /**
     * 是否为 重试前缀开头的 topic
     *
     * @param resource topic
     * @return boolean
     */
    public static boolean isRetryTopic(String resource) {
        return StringUtils.isNotBlank(resource) && resource.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    /**
     * 是否为 死信前缀开头的 topic
     *
     * @param resource topic
     * @return boolean
     */
    public static boolean isDLQTopic(String resource) {
        return StringUtils.isNotBlank(resource) && resource.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
    }
}