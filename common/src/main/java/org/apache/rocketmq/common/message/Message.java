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
package org.apache.rocketmq.common.message;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {
    private static final long serialVersionUID = 8445773977080406428L;

    // 主题
    private String topic;
    // 消息标记（RocketMQ不做处理） @see org.apache.rocketmq.common.sysflag.MessageSysFlag
    private int flag;
    /**
     * 扩展属性
     * <p>
     * KEYS: 消息索引建，用空格隔开，RocketMQ 可以根据这些 key 快速检索消息。
     * TAGS: 消息标签，用于消息过滤。
     * DELAY: 延迟时间级别。
     * WAIT: 消息发送时是否等消息存储完成后再返回。
     * <p>
     * ...
     * ...
     */
    private Map<String, String> properties;
    // 消息体
    private byte[] body;
    // 事务消息相关的事务编号
    private String transactionId;

    public Message() {
    }

    public Message(String topic, byte[] body) {
        this(topic, "", "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
        this.topic = topic;
        this.flag = flag;
        this.body = body;

        if (tags != null && tags.length() > 0)
            this.setTags(tags);

        if (keys != null && keys.length() > 0)
            this.setKeys(keys);

        this.setWaitStoreMsgOK(waitStoreMsgOK);
    }

    public Message(String topic, String tags, byte[] body) {
        this(topic, tags, "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, byte[] body) {
        this(topic, tags, keys, 0, body, true);
    }

    public void setKeys(String keys) {
        this.putProperty(MessageConst.PROPERTY_KEYS, keys);
    }

    void putProperty(final String name, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<String, String>();
        }

        this.properties.put(name, value);
    }

    void clearProperty(final String name) {
        if (null != this.properties) {
            this.properties.remove(name);
        }
    }

    public void putUserProperty(final String name, final String value) {
        if (MessageConst.STRING_HASH_SET.contains(name)) {
            throw new RuntimeException(String.format(
                    "The Property<%s> is used by system, input another please", name));
        }

        if (value == null || value.trim().isEmpty()
                || name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "The name or value of property can not be null or blank string!"
            );
        }

        this.putProperty(name, value);
    }

    public String getUserProperty(final String name) {
        return this.getProperty(name);
    }

    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<String, String>();
        }

        return this.properties.get(name);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTags() {
        return this.getProperty(MessageConst.PROPERTY_TAGS);
    }

    public void setTags(String tags) {
        this.putProperty(MessageConst.PROPERTY_TAGS, tags);
    }

    public String getKeys() {
        return this.getProperty(MessageConst.PROPERTY_KEYS);
    }

    public void setKeys(Collection<String> keys) {
        StringBuffer sb = new StringBuffer();
        for (String k : keys) {
            sb.append(k);
            sb.append(MessageConst.KEY_SEPARATOR);
        }

        this.setKeys(sb.toString().trim());
    }

    public int getDelayTimeLevel() {
        String t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (t != null) {
            return Integer.parseInt(t);
        }

        return 0;
    }

    public void setDelayTimeLevel(int level) {
        this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
    }


    /**
     * 用于获取消息是否等待存储完成的状态。
     * <p>
     * 在 RocketMQ 中，当一个消息发送到 Broker 后，Producer 可以选择等待消息在 Broker 上写入完成，再继续执行发送操作。
     * 这个方法用于获取消息属性中的 PROPERTY_WAIT_STORE_MSG_OK 属性值，该属性表示消息是否等待存储完成，如果未设置该属性则默认为等待存储完成。
     * 返回值为 boolean 类型，表示是否等待消息存储完成。如果返回 true，则表示等待消息存储完成，否则不等待。
     * 这个方法主要在消息发送和消费过程中使用，用于控制消息存储和消费的同步和异步模式。
     *
     * @return boolean
     */
    public boolean isWaitStoreMsgOK() {
        String result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        if (null == result)
            return true;

        return Boolean.parseBoolean(result);
    }

    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, Boolean.toString(waitStoreMsgOK));
    }

    public void setInstanceId(String instanceId) {
        this.putProperty(MessageConst.PROPERTY_INSTANCE_ID, instanceId);
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getBuyerId() {
        return getProperty(MessageConst.PROPERTY_BUYER_ID);
    }

    public void setBuyerId(String buyerId) {
        putProperty(MessageConst.PROPERTY_BUYER_ID, buyerId);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", flag=" + flag +
                ", properties=" + properties +
                ", body=" + Arrays.toString(body) +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }
}
