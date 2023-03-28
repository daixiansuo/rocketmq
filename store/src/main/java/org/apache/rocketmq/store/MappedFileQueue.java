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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;


/**
 * MappedFileQueue 是 RocketMQ 中存储消息的核心组件之一，它负责管理消息存储的物理文件，实现了消息的落盘和加载。
 * 其主要作用是将消息写入到磁盘中，同时支持快速的随机读写，确保消息的可靠存储和快速访问。
 * <p>
 * 设计思想上，MappedFileQueue 基于内存映射技术，将磁盘上的文件映射到进程的虚拟内存空间中，利用操作系统的缓存和预读技术来提高 IO 效率。
 * MappedFileQueue 对消息存储和查询性能做了优化，支持快速的追加消息和随机读取，同时实现了高可靠性的消息存储和快速访问。
 * <p>
 * MappedFileQueue 的使用场景主要是在消息存储过程中，通过内存映射技术来加速读写消息，提高存储性能。
 * 它主要被应用在消息存储、消息索引、消息追踪等场景中。
 * 在 RocketMQ 中，MappedFileQueue 被广泛应用于 CommitLog、ConsumeQueue、IndexFile 等存储组件中，可以有效地提升 RocketMQ 的性能和可靠性。
 */
public class MappedFileQueue {

    // 日志记录器，用于记录普通日志。
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 日志记录器，用于记录错误日志。
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    // 一次删除最大的文件数量，默认值为 10。
    private static final int DELETE_FILES_BATCH_MAX = 10;

    // 存储文件的路径
    private final String storePath;

    // 单个文件的大小
    private final int mappedFileSize;

    /**
     * 已经分配的内存映射文件列表
     * <p>
     * CopyOnWriteArrayList 在添加元素时会按照添加的顺序进行排序，并且保持这个顺序。
     * 虽然 CopyOnWriteArrayList 不是线程安全的，但是它保证了每次添加元素都会在底层数组的末尾添加，因此新元素的顺序始终与添加顺序相同
     */
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    // 内存映射文件分配服务
    private final AllocateMappedFileService allocateMappedFileService;

    // 当前刷盘指针，表示该指针之前的所有数据全部持久化到磁盘。（已经刷写到磁盘的位置）
    private long flushedWhere = 0;
    // 当前数据提交指针，内存中ByteBuffer当前的写指针，该值大于、等于flushedWhere。（已经提交的位置）
    private long committedWhere = 0;
    // 存储时间戳。
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }


    /**
     * MappedFile 主要用于 CommitLog、ConsumeQueue、IndexFile 文件的映射，这些类型的文件大小都是固定，且文件名称都是以第一条内容的偏移量来命名。
     * <p>
     * 所以，遍历所有的映射文件，检查 当前MappedFile文件的起始偏移量 与 上一个MappedFile文件的起始偏移量 相差是否等于 mappedFileSize。
     * 如果不相等，则表明 映射文件队列的数据已损坏，相邻映射文件的偏移量不匹配。
     */
    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            // 映射文件迭代器
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            // 记录上一个文件指针
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    // 检查 当前MappedFile文件的起始偏移量 与 上一个MappedFile文件的起始偏移量 相差是否等于 mappedFileSize。
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }


    /**
     * 根据传入的时间戳，从 mappedFiles 中获取最后修改时间晚于传入时间戳的 MappedFile 对象，如果队列中没有任何 MappedFile，则返回 null。
     * <p>
     * 该方法通常用于消费消息时，根据消息的时间戳查找对应的消息存储文件，从而读取对应的消息数据。
     *
     * @param timestamp 指定时间戳
     * @return MappedFile
     */
    public MappedFile getMappedFileByTime(final long timestamp) {

        // 复制一份 mappedFiles 数据
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        // 遍历所有 MappedFile 文件
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            // 判断当前 MappedFile 的最后修改时间 是否晚于传入时间戳，如果是，则返回该 MappedFile 。
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        // 如果没有找到，则返回数组中最后一个 MappedFile。
        return (MappedFile) mfs[mfs.length - 1];
    }


    /**
     * 将MappedFileQueue中的MappedFile对象拷贝到一个新的Object数组中并返回。
     * <p>
     * 如果MappedFileQueue中的MappedFile对象数小于或等于保留的数量，则返回null。
     * 否则，将MappedFileQueue中的MappedFile对象复制到Object数组中，并返回该数组
     *
     * @param reservedMappedFiles 表示保留的MappedFile数量
     * @return Object[]
     */
    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }


    /**
     * 方法的作用是将文件队列中写入指定偏移量之后的数据进行截断，保留指定偏移量之前的数据
     * <p>
     * 具体实现是遍历文件队列中的每个文件，判断文件尾部偏移量是否大于指定偏移量，若是，则对该文件进行截断操作。
     * 对于需要截断的文件，如果其起始偏移量大于指定偏移量，则将该文件从文件队列中删除。
     * <p>
     * 该方法的使用场景是在处理文件数据时，需要删除文件队列中部分数据，保留指定偏移量之前的数据，
     * 比如在消息消费时，如果已经消费完部分数据，需要将该部分数据从文件队列中删除，以便释放存储空间。
     *
     * @param offset 偏移量
     */
    public void truncateDirtyFiles(long offset) {

        // 用于存储需要删除的 MappedFile 对象，即文件大小超过偏移量的文件。
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        // 遍历所有 MappedFile 文件
        for (MappedFile file : this.mappedFiles) {

            // 计算 MappedFile 的结束偏移量
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            // 如果当前文件的结束偏移量大于指定的偏移量，说明需要进行截断操作。
            if (fileTailOffset > offset) {
                // 如果指定的偏移量大于或等于当前文件的起始偏移量，说明需要将当前文件截断到指定偏移量。
                if (offset >= file.getFileFromOffset()) {
                    // 将当前文件的写入、提交和刷盘位置设置为指定偏移量。
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {

                    // 如果指定的偏移量小于当前文件的起始偏移量，则需要将当前文件删除。

                    // 调用 destroy 方法将当前文件从磁盘中删除
                    file.destroy(1000);
                    // 将当前文件对象加入到 willRemoveFiles 列表中
                    willRemoveFiles.add(file);
                }
            }
        }

        // 删除所有标记为删除的 MappedFile 文件。
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 删除过期文件
     *
     * @param files 待删除文件列表
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                // 删除
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }


    /**
     * 方法用于从指定路径中加载MappedFile文件，并将其添加到mappedFiles列表中。
     *
     * @return boolean
     */
    public boolean load() {

        // 获取指定存储路径的文件对象
        File dir = new File(this.storePath);
        // 获取当前目录下的所有 commitLog 文件
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 按照文件名的升序排序
            Arrays.sort(files);
            for (File file : files) {

                // 如果文件大小 不正确，返回 false，直接退出。
                // TODO：意味着 只加载完成的文件 ！！！ 对于大小不对的文件不加载 ！！
                if (file.length() != this.mappedFileSize) {
                    // 长度不匹配消息存储库配置值，请手动检查
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {

                    // 创建一个新的MappedFile对象
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    // MappedFile对象的写入、刷盘和提交位置都设置为mappedFileSize，因为文件加载后尚未写入任何数据。
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    // 将其添加到mappedFiles列表中。
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 方法作用是获取最后一个MappedFile对象。如果需要创建新的MappedFile，则创建并返回新的对象，否则返回最后一个MappedFile对象。
     * <p>
     * 使用场景：当消息需要写入磁盘时，需要先确定将消息写入哪个MappedFile对象。
     * 因此，在调用这个方法之前，消息队列通常会先检查最后一个MappedFile对象是否已满。
     * 如果已满，需要创建一个新的MappedFile对象，并将新的对象添加到MappedFile队列中。
     *
     * @param startOffset 开始偏移量
     * @param needCreate  是否需要创建新的 MappedFile
     * @return MappedFile
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {

        // 定义一个变量 createOffset，并初始化为 -1
        long createOffset = -1;
        // 获取当面队列中 最后一个 MappedFile
        MappedFile mappedFileLast = getLastMappedFile();

        /*
            如果当前 mappedFileLast 为 null，说明当前还没有任何 MappedFile 被创建，需要创建一个新的 MappedFile。
            此时通过对 startOffset 取模获取到当前的偏移量，然后将其减去模数，得到下一个 MappedFile 的起始偏移量 createOffset。
         */
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        /*
            如果当前的 mappedFileLast 不为 null，则检查其是否已经写满。
            如果是，则根据 mappedFileLast 的起始偏移量和 MappedFile 大小计算下一个 MappedFile 的起始偏移量 createOffset。
         */
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        // 如果需要创建新的 MappedFile，且下一个 MappedFile 的起始偏移量 createOffset 已经被计算出来，那么根据 createOffset 构造新的 MappedFile，并将其添加到 MappedFile 队列中
        if (createOffset != -1 && needCreate) {

            // 文件名称
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            // TODO： 作用？
            String nextNextFilePath = this.storePath + File.separator
                    + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;

            // 默认 allocateMappedFileService 不为空，使用 allocateMappedFileService 创建 MappedFile。
            if (this.allocateMappedFileService != null) {
                // 创建 MappedFile
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                        nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            // 不为空则创建成功
            if (mappedFile != null) {
                // 如果此时 MappedFile 队列为空，将该 MappedFile 标记为第一个被创建的 MappedFile
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                // 添加到 queue
                this.mappedFiles.add(mappedFile);
            }

            // 返回新创建的 MappedFile
            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个 MappedFile
     *
     * @return MappedFile
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        /**
         * 使用 while 循环的主要目的是为了解决并发修改 mappedFiles 集合可能引发的 IndexOutOfBoundsException 异常。
         * 考虑到该方法可能被多线程调用，并且同时也有其他线程可能会向 mappedFiles 集合中添加或删除元素，
         * 因此如果直接获取最后一个元素，那么可能会在遍历过程中出现并发修改导致的越界异常。
         * 因此使用 while 循环可以在出现异常时进行重试，直到成功获取最后一个元素为止，从而保证了线程安全
         */
        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }


    /**
     * 该方法是用来重置消息存储文件的写入位置和提交位置的。
     * <p>
     * 在一些异常情况下（例如消息消费出现异常或网络中断），可能会导致消息存储文件的写入位置和提交位置不一致，导致消息被重复消费或者消息丢失。
     * 此时可以通过调用该方法，重置存储文件的写入位置和提交位置，使其重新回到指定的 offset 处。
     * <p>
     * 该方法的使用场景一般是在消费者端，当消费者在消费消息时，发现某些消息出现异常，需要重置存储文件的写入位置和提交位置，以便重新消费该批消息。
     *
     * @param offset 需要重置到的 偏移量
     * @return boolean
     */
    public boolean resetOffset(long offset) {

        // 获取队列中最后一个 MappedFile
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {

            // 已写入到MappedFile中的数据位置。
            long lastOffset = mappedFileLast.getFileFromOffset() +
                    mappedFileLast.getWrotePosition();
            // 计算与 给定偏移量 之间的差值。
            long diff = lastOffset - offset;

            // 最大差异
            final int maxDiff = this.mappedFileSize * 2;

            // 将最后一个MappedFile的偏移量与给定偏移量进行比较，如果两者之间的差大于mappedFileSize的两倍，则返回false。
            // 这是为了防止将偏移量设置到一个不存在的位置。
            if (diff > maxDiff)
                return false;
        }

        // 迭代器
        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();
        // 逆向遍历 MappedFile 队列， 即从文件队列的最后一个文件开始向前遍历，
        while (iterator.hasPrevious()) {
            // 获取上一个元素
            mappedFileLast = iterator.previous();

            // 判断 给定偏移量是否 大于等于 当前文件的起始偏移量
            if (offset >= mappedFileLast.getFileFromOffset()) {

                // mappedFileLast.getFileSize() == mappedFileSize （即单个文件的大小，例如 commitLog：1G）
                // 取模计算 偏移量
                int where = (int) (offset % mappedFileLast.getFileSize());

                // 将当前文件的写指针设置为给定偏移量，同时将该文件的刷盘位置和提交位置都设置为该偏移量
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                // 如果偏移量小于当前文件的起始偏移量，则将该文件从队列中删除 ！！！
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 该方法的作用是获取当前存储文件中最小的偏移量。
     * <p>
     * 偏移量是存储在MappedFile的文件名中的，因此此方法只需获取第一个MappedFile的起始偏移量即可。如果当前MappedFile列表为空，则返回-1。
     * <p>
     * 使用场景是当需要查找或检查当前存储文件中最小的偏移量时，可以调用该方法。例如，在消费消息时，需要确定开始消费的位置，可以通过该方法获取最小偏移量
     *
     * @return 最小的偏移量
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }


    /**
     * 该方法用于获取当前最大的消息偏移量，即最后一个文件的起始偏移量 加上读指针所在的位置。如果当前还没有任何消息，则返回0。
     * <p>
     * 使用场景：在消息存储过程中，需要获取当前最大的消息偏移量，方便后续读取数据。
     *
     * @return 最大的消息偏移量
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }


    /**
     * 返回当前 MappedFile队列 中最后一个MappedFile的 起始offset 加上 其已经写入的字节数，也就是最大写入位置。
     *
     * @return 最大写入位置
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }


    /**
     * 这个方法用于计算当前还有多少数据尚未提交
     * <p>
     * 即当前最大已写入位置减去已提交位置。
     */
    public long remainHowManyDataToCommit() {

        /*
            // 获取当前最大已写入位置
            long maxWrotePosition = getMaxWrotePosition();
            // 计算尚未提交的数据大小
            long remainToCommit = maxWrotePosition - committedWhere;
             return remainToCommit;
         */

        return getMaxWrotePosition() - committedWhere;
    }

    /**
     * 用于计算还有多少数据需要刷盘
     * <p>
     * 即未被刷盘的数据量
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    /**
     * 删除最后一个 MappedFile，即列表中最后一个元素
     * <p>
     * 当 RocketMQ 服务器宕机后重新启动，恢复数据的时候，可能需要删除一些无用的 MappedFile 文件。
     * 在这种情况下，调用 deleteLastMappedFile() 方法可以将最后一个 MappedFile 文件从磁盘上删除，并且从 mappedFiles 集合中移除。
     */
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            // 销毁方法中的参数表示等待销毁操作的最长时间，单位为毫秒。
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }


    /**
     * 删除过期的文件，即根据文件的最后修改时间和指定的过期时间，判断是否需要删除文件。
     * <p>
     * 该方法可用于清理磁盘上的无用文件，防止磁盘空间不足。
     *
     * @param expiredTime         过期时间，单位为毫秒。 eg：commitLog文件时传入的值是 72h 对应的毫秒数。
     *                            72h 这个是默认值，在MessageStoreConfig 中配置，可以通过配置文件修改。
     *                            所以，expiredTime 并不是一个时间点，而是一个 时间长度！！
     * @param deleteFilesInterval 删除文件间隔时间，单位为毫秒，表示每删除一个文件后暂停的时间。
     * @param intervalForcibly    强制删除间隔时间，单位为毫秒，表示每次尝试删除文件的间隔时间。
     * @param cleanImmediately    是否立即清理文件，若为true，则无论是否过期都会立即删除。
     * @return 删除的文件数。
     */
    public int deleteExpiredFileByTime(final long expiredTime,
                                       final int deleteFilesInterval,
                                       final long intervalForcibly,
                                       final boolean cleanImmediately) {

        // 获取当前队列中所有的 MappedFile 对象。
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        // TODO： 为什么要 -1 ？
        // 当前队列中 MappedFile 对象的数量。
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            // 遍历所有 MappedFile 文件
            for (int i = 0; i < mfsLength; i++) {

                MappedFile mappedFile = (MappedFile) mfs[i];
                // 计算最大存活时间 = 最后修改时间 + expiredTime
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                // 判断当前系统时间 是否超过 最大存活时间，如果大于说明 过期需要清理。 cleanImmediately 表示 立即清理文件，无论是否过期都会立即删除
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    // 销毁 MappedFile 文件 （destroy方法中会删除这个文件）
                    if (mappedFile.destroy(intervalForcibly)) {

                        // 添加到要删除的文件列表中
                        files.add(mappedFile);
                        // 删除计数
                        deleteCount++;

                        // 一次删除最大的文件数量， 如果超过，下次在删除呗
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        // 如果 deleteFilesInterval 大于0且当前文件不是最后一个文件，则等待 deleteFilesInterval 毫秒再继续删除操作
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                // 等待阻塞
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        // 如果删除失败，则停止删除操作。
                        break;
                    }
                } else {
                    // 如果该文件的最后修改时间距离现在没有超过过期时间，则停止删除操作。
                    break;
                }
            }
        }

        // 删除队列中的数据
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 根据队列偏移量删除过期的文件，返回删除的文件数目
     *
     * @param offset   队列偏移量
     * @param unitSize 单位大小
     * @return 删除的文件数目
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {

        // 复制映射文件
        Object[] mfs = this.copyMappedFiles(0);

        // 待删除的文件、删除文件数量
        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            // 获取映射文件长度
            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 从映射文件中选择最后一个单位大小. eg: consumeQueue 文件条目 单个条目 20字节 ！！
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    // 获取逻辑队列中的最大偏移量
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    // 判断当前映射文件中的最大偏移量 是否小于传入的偏移量
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                // 如果需要删除并且删除成功，则添加到文件列表中
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    // 添加到将要删除的文件列表
                    files.add(mappedFile);
                    // 增加计数
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        // 删除过期文件
        deleteExpiredFile(files);

        return deleteCount;
    }


    /**
     * 刷盘
     *
     * @param flushLeastPages 至少需要刷几个 page
     * @return 是否刷盘成功
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 根据偏移量找到对应的 MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            // 获取文件存储时间戳，刷盘偏移量 （最后一次更新MappedFile时间戳）
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            // 核心: 刷盘 !!!
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            // 判断是否刷盘成功
            result = where == this.flushedWhere;
            // 更新已刷盘位置
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                // 如果 flushLeastPages 为 0，更新存储时间戳
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 主要用在开启短暂存储池时，主要用于支持瞬态存储池，将消息数据从瞬态存储池中刷写到磁盘中。
     * invoke：org.apache.rocketmq.store.CommitLog#commitLogService
     * - org.apache.rocketmq.store.CommitLog.CommitRealTimeService#run()
     *
     * @param commitLeastPages 最少需要刷盘的页数。
     * @return boolean
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        // 根据 committedWhere 找到对应的 mappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            // 从 mappedFile 刷盘，获取刷盘后的写指针
            int offset = mappedFile.commit(commitLeastPages);
            // 计算刷盘后的最新写指针位置
            long where = mappedFile.getFileFromOffset() + offset;
            // 检查最新写指针位置是否正确，并更新 committedWhere 指针
            result = where == this.committedWhere;
            // 已经提交的位置
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * 根据给定的偏移量查找对应的 MappedFile，如果找到则返回该文件，否则根据 returnFirstOnNotFound 参数决定是否返回第一个文件或者返回 null。
     *
     * @param offset                要查找的偏移量
     * @param returnFirstOnNotFound 如果没有找到对应偏移量的文件，是否返回第一个文件
     * @return 如果找到对应偏移量的文件，则返回该文件；否则，如果 returnFirstOnNotFound=true，则返回第一个文件，否则返回 null
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {

            // 第一个MappedFile文件
            MappedFile firstMappedFile = this.getFirstMappedFile();
            // 最后一个MappedFile文件
            MappedFile lastMappedFile = this.getLastMappedFile();
            // 都不为空，则判断偏移量是否超出文件的范围，即是否为有效的 offset
            if (firstMappedFile != null && lastMappedFile != null) {
                // 判断偏移量是否超出文件的范围
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    // 如果偏移量超出文件范围，则记录日志
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                } else { // 合法的offset

                    // 计算目标 MappedFile 的下标
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        // 获取目标 MappedFile
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    // 如果找到目标 MappedFile，则返回该文件
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    // 否则，循环遍历所有 MappedFile，如果找到对应偏移量的文件，则返回该文件
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                // 如果找不到对应偏移量的文件，并且 returnFirstOnNotFound=true，则返回第一个文件
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }


    /**
     * 获取队列中的 第一个MappedFile文件
     *
     * @return MappedFile
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    /**
     * 获取 映射内存 的大小！！
     * <p>
     * 注：只统计 可用状态的 MappedFile 文件
     *
     * @return long
     */
    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                // 只统计 可用状态的 MappedFile 文件
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }


    /**
     * 重试删除存储路径下的第一个MappedFile文件
     * <p>
     * 使用场景一般是在存储路径中第一个文件已经被销毁，但是因为一些异常导致文件没有被正常删除。
     * 此时调用该方法可以尝试重新删除该文件，避免文件残留在存储路径下，占用磁盘空间。
     *
     * @param intervalForcibly 表示强制删除文件的时间间隔，单位为毫秒。
     *                         在删除文件时，如果该文件被其他线程占用或存在IO操作未完成，就需要等待一段时间后再次尝试删除该文件，间隔时间即为intervalForcibly。
     *                         如果超过一定时间仍无法删除该文件，则会记录错误日志。
     * @return boolean
     */
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                // 注销删除
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    // 删除过期的MappedFile
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }


    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    /**
     * 销毁
     */
    public void destroy() {

        // 遍历所有 MappedFile 文件，逐个注销
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }

        // 清空队列
        this.mappedFiles.clear();
        // 重置已经刷写到磁盘的位置
        this.flushedWhere = 0;

        // delete parent directory
        // 删除父级目录
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
