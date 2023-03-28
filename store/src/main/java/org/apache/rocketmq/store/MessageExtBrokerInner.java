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

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageExt;


/**
 * 消息扩展模型 适用于Broker内部。
 */
public class MessageExtBrokerInner extends MessageExt {

    private static final long serialVersionUID = 7256001576878700634L;

    // 属性字段
    private String propertiesString;
    // 消息标签的 hashCode 值
    private long tagsCode;


    /**
     * 用来将 tags 字符串转换为 tagsCode 的
     *
     * @param filter 主题过滤的类型
     * @param tags   标签列表字符串
     * @return long
     */
    public static long tagsString2tagsCode(final TopicFilterType filter, final String tags) {
        if (null == tags || tags.length() == 0) {
            return 0;
        }

        return tags.hashCode();
    }

    public static long tagsString2tagsCode(final String tags) {
        return tagsString2tagsCode(null, tags);
    }

    public String getPropertiesString() {
        return propertiesString;
    }

    public void setPropertiesString(String propertiesString) {
        this.propertiesString = propertiesString;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }
}
