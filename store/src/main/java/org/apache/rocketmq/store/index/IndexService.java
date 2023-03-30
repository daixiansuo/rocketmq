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
package org.apache.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;


/**
 * IndexService 类是消息索引服务的核心组件，主要负责管理和维护所有的消息索引。
 * <p>
 * 它会根据消息的 key 和 tags，计算出一个 hash code，然后将该消息索引记录在对应的 hash slot 中。
 * IndexService 类还会定期将内存中的消息索引刷写到磁盘上，以保证消息索引的持久化存储。
 * 设计思想是将索引文件拆分成多个小文件，方便管理和维护。同时使用读写锁保证索引文件的并发访问安全，提高系统性能。
 */
public class IndexService {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 最大尝试创建索引文件的次数。若创建索引文件失败，则会进行重试。
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;

    // 默认存储实现，相互持有
    private final DefaultMessageStore defaultMessageStore;
    // 哈希槽数量
    private final int hashSlotNum;
    // 索引条目的数量。
    private final int indexNum;
    // 存储路径
    private final String storePath;
    // 索引文件列表，记录了所有创建的索引文件。
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    // 读写锁，用于保证索引文件的并发访问安全。
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        // 默认哈希槽数量 500w
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        // 默认索引条目数量 = 哈希槽数量 * 4 = 2000w
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        // 存储路径
        this.storePath =
                StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 加载数据
     *
     * @param lastExitOK 最后一次退出是否正常
     * @return boolean
     */
    public boolean load(final boolean lastExitOK) {

        // 存储目录
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 根据文件名称拍下
            Arrays.sort(files);
            // 遍历所有 Index 文件
            for (File file : files) {
                try {
                    // 创建 IndexFile
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    // 加载数据，主要是读取文件头信心
                    f.load();

                    // 最后一次是 异常退出，即没有触发 JVM钩子函数，没能执行销毁流程。
                    if (!lastExitOK) {
                        // 如果当前Index文件中消息的最大存储时间 > 文件检查点当中记录的时间，则销毁删除文件
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                                .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    // 添加到文件列表
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }


    /**
     * 删除过期文件
     *
     * @param offset commitLog 最小物理偏移量
     */
    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            // 加读锁
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            // 获取第一个 IndexFile ，然后获取该 IndexFile 文件中包含消息的最大 CommitLog 文件偏移量
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            // 判断是否消息 指定的物理偏移量
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            // 释放读锁
            this.readWriteLock.readLock().unlock();
        }

        // 如果不为空，说明有 IndexFile 文件存在过期数据，需要删除
        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            // 遍历所有文件
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                // 判断是否需要删除，即当前 Index文件中的 最大物理偏移量 小于 指定的偏移量。
                if (f.getEndPhyOffset() < offset) {
                    // 添加到待删除文件列表
                    fileList.add(f);
                } else {
                    break;
                }
            }

            // 删除过期文件
            this.deleteExpiredFile(fileList);
        }
    }

    /**
     * 删除指定的过期文件列表
     *
     * @param files 过期文件列表
     */
    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                // 加写锁
                this.readWriteLock.writeLock().lock();
                // 遍历销毁删除
                for (IndexFile file : files) {
                    // 销毁
                    boolean destroyed = file.destroy(3000);
                    // 删除
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    /**
     * 销毁
     */
    public void destroy() {
        try {
            // 写锁
            this.readWriteLock.writeLock().lock();
            // 遍历销毁
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            // 清空
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            // 释放锁
            this.readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 查询指定主题、键以及时间范围内的消息物理偏移量，并返回查询结果。
     *
     * @param topic  主题
     * @param key    key
     * @param maxNum 最大返回消息数量
     * @param begin  开始时间
     * @param end    结束时间
     * @return QueryOffsetResult
     */
    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {

        // 定义一个 List 用于存储查找到的物理偏移量
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        // 定义变量，记录索引文件列表中最后一个文件的更新时间戳和物理偏移量
        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;

        // 将 maxNum 设置为用户指定的最大消息数量 和 消息存储配置中指定的最大批量数量(默认64) 的最小值
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            // 加上读锁，防止在查询的过程中有写入操作
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                // 倒序遍历索引文件列表中的文件
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    // 取出一个索引文件
                    IndexFile f = this.indexFileList.get(i - 1);
                    // 是否是最后一个文件
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        // 最后一个文件的更新时间戳和物理偏移量
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    // 如果该索引文件的时间范围包含查询范围，则调用该索引文件的 selectPhyOffset 方法查找符合要求的物理偏移量
                    if (f.isTimeMatched(begin, end)) {
                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    // 如果该索引文件的起始时间戳小于查询范围的起始时间戳，则说明该文件已经不能满足查询要求了，直接退出循环
                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    // 如果查找到的物理偏移量的数量已经达到 maxNum，则退出循环
                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        // 返回查询结果
        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }


    /**
     * CommitLog 分发请求，构建 index 条目数据。
     * <p>
     * invoke: org.apache.rocketmq.store.DefaultMessageStore.CommitLogDispatcherBuildIndex
     *
     * @param req 分发请求
     */
    public void buildIndex(DispatchRequest req) {

        // 获取或创建Index文件并获取所有文件最大的物理偏移量
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {

            // 当前 indexFile 文件中，持有最大的 commitLogOffset
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            String topic = msg.getTopic();
            String keys = msg.getKeys();

            // 如果该消息的物理偏移量小于Index文件中的物理偏移量，则说明是重复数据，忽略本次索引构建，
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            // 消息类型
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            // 如果消息的唯一键不为空，则添加到哈希索引中，以便加速根据唯一键检索消息
            if (req.getUniqKey() != null) {
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            // 构建索引键，RocketMQ支持为同一个消息建立多个索引，多个索引键用空格分开。
            if (keys != null && keys.length() > 0) {
                // 空格分割
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        // 添加数据
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    /**
     * 向Index文件中 添加消息索引
     *
     * @param indexFile 索引文件
     * @param msg       消息
     * @param idxKey    topic+key
     * @return IndexFile
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {

        // 使用循环语句，不断尝试将索引键值对插入当前的索引文件，直到插入成功。
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            // 如果当前索引文件已满，将会通过 retryGetAndCreateIndexFile() 方法获取新的索引文件，然后将索引键值对插入新的索引文件。
            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            // 继续插入数据
            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * 重试获取或创建索引文件，返回创建的 IndexFile 对象或 null。
     * <p>
     * 该方法使用一个循环，最多重试 MAX_TRY_IDX_CREATE 次，尝试获取或创建最后一个索引文件。
     * 如果成功获取或创建索引文件，则返回该文件对应的 IndexFile 对象；
     * 如果尝试 MAX_TRY_IDX_CREATE 次都没有获取或创建成功，则记录无法创建索引文件的标志并返回 null。
     * <p>
     * Retries to get or create index file.
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        // MAX_TRY_IDX_CREATE 最大尝试创建索引文件的次数。若创建索引文件失败，则会进行重试
        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            // 获取或创建最后一个索引文件
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile)
                break;

            try {
                // 在重试过程中，每次尝试创建索引文件失败后，线程会休眠 1 秒钟。
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            // 记录无法创建索引文件的标志
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 获取或创建最后一个索引文件
     * <p>
     * 它首先获取索引文件列表中的最后一个文件，如果该文件未满，则返回该文件。
     * 否则，它会记录最后一个索引文件的偏移量和时间戳，并将其存储在变量lastUpdateEndPhyOffset和lastUpdateIndexTimestamp中。
     * 如果没有找到未满的文件，它将创建一个新的索引文件，并将其添加到索引文件列表中
     * <p>
     * 创建新文件后，它会启动一个线程来将旧文件刷新到磁盘上。这个方法是为了保证在索引文件列表中始终有一个可用的文件来存储索引数据。
     *
     * @return IndexFile
     */
    public IndexFile getAndCreateLastIndexFile() {

        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            // 加读锁
            this.readWriteLock.readLock().lock();
            // 索引文件列表不为空
            if (!this.indexFileList.isEmpty()) {
                // 获取最后一个 IndexFile 文件
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                // 如果没写满，则返回这个 indexFile 文件。
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                } else {
                    // 如果写满，则记录最后一个索引文件的物理偏移量和时间戳，并保存为 prevIndexFile。
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            // 释放读锁
            this.readWriteLock.readLock().unlock();
        }

        // 如果 indexFile 为空，说明index文件列表为空，或者最后一个写满了。
        if (indexFile == null) {
            try {
                // 文件名称
                String fileName =
                        this.storePath + File.separator
                                + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                // 新建IndexFile文件
                indexFile =
                        new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                                lastUpdateIndexTimestamp);

                // 加写锁
                this.readWriteLock.writeLock().lock();
                // 添加到 indexFileList 集合中。
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                // 释放写锁
                this.readWriteLock.writeLock().unlock();
            }

            // 如果成功创建索引文件，则启动一个后台线程 FlushIndexFileThread，用于定时刷新索引文件到磁盘。
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // 刷盘
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }


    /**
     * 将一个索引文件刷盘，保证其中的索引数据持久化到磁盘上
     *
     * @param f 索引文件
     */
    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        // 如果索引文件已经写满（即已经存储的索引个数达到了索引文件的容量上限），则将其结尾的最后一个索引记录的时间戳记录下来，后面会用到
        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        // 调用索引文件的 flush 方法，将其中的索引数据刷写到磁盘上。
        f.flush();

        // 如果第 2 步中已经记录了索引文件中最后一个索引记录的时间戳
        if (indexMsgTimestamp > 0) {
            // 更新消息存储检查点中的索引消息时间戳，并将其刷盘
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
