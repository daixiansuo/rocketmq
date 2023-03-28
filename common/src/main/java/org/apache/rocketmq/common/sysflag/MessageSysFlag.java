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
package org.apache.rocketmq.common.sysflag;


/**
 * MessageSysFlag 类的常量，通过按位或运算可以组合使用。
 * <p>
 * 例如，如果消息被压缩过，并且包含多个标签，则可以将 COMPRESSED_FLAG 和 MULTI_TAGS_FLAG 两个属性的值进行按位或运算，得到一个包含这两个属性的值。
 */
public class MessageSysFlag {

    //压缩标志位，表示消息是否被压缩过
    public final static int COMPRESSED_FLAG = 0x1;

    // 多标签标志位，表示消息是否包含多个标签。
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;

    // 事务类型，表示非事务消息。
    public final static int TRANSACTION_NOT_TYPE = 0;

    // 事务类型，表示事务消息的 Prepared 状态。
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;

    // 事务类型，表示事务消息的 Commit 状态。
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;

    // 事务类型，表示事务消息的 Rollback 状态。
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

    // Born Host v6 标志位，表示 Born Host 是否为 IPv6 地址。
    public final static int BORNHOST_V6_FLAG = 0x1 << 4;

    // Store Host Address v6 标志位，表示 Store Host 是否为 IPv6 地址。
    public final static int STOREHOSTADDRESS_V6_FLAG = 0x1 << 5;




    /*

        &：按位与运算符，将两个数的二进制位进行与运算，只有相应位都为 1 时结果才为 1，否则为 0。

           例如，flag & TRANSACTION_ROLLBACK_TYPE 表示将 flag 和 TRANSACTION_ROLLBACK_TYPE 进行按位与运算，得到事务类型的值。

        |：按位或运算符，将两个数的二进制位进行或运算，只有相应位都为 0 时结果才为 0，否则为 1。

           例如，COMPRESSED_FLAG | MULTI_TAGS_FLAG 表示将 COMPRESSED_FLAG 和 MULTI_TAGS_FLAG 进行按位或运算，得到同时包含这两个属性的值。

        <<：左移运算符，将一个数的二进制位向左移动指定的位数，高位补 0。

           例如，0x1 << 4 表示将二进制数 0001 向左移动 4 位，得到二进制数 00010000，即十进制数 16。

     */



    /**
     * 从标志位中获取事务类型，即将标志位中除 TRANSACTION_ROLLBACK_TYPE 以外的部分清零，得到事务类型的值。
     *
     * @param flag 标志位
     * @return 标志位
     */
    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }


    /**
     * 重置标志位中的事务类型，即将标志位中除 TRANSACTION_ROLLBACK_TYPE 以外的部分清零，然后将指定的事务类型值合并到标志位中。
     *
     * @param flag 需要重置的事物类型
     * @param type 需要合并的标志位
     * @return int
     */
    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }


    /**
     * 清除标志位中的压缩标志，即将 COMPRESSED_FLAG 部分清零。
     *
     * @param flag 标志位
     * @return int
     */
    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }

}
