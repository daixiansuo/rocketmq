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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * MappedFile 是 RocketMQ 消息存储核心组件，用于将消息快速写入磁盘和从磁盘读取消息。
 * <p>
 * 它的设计思想是采用内存映射技术，将文件映射到内存中，然后通过直接内存操作来进行读写，避免了频繁的磁盘 I/O 操作，提高了读写性能。其主要作用包括：
 * 1）提供了内存映射文件的操作接口，支持消息的读写和文件的刷盘等操作；
 * 2）管理内存映射文件和磁盘文件的生命周期，包括文件的创建、删除、切换等操作；
 * 3）提供了内存池功能，用于管理内存的分配和回收，避免了频繁的内存申请和释放操作，提高了性能和稳定性。
 */
public class MappedFile extends ReferenceResource {

    // 存储日志的内部日志记录器。
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 操作系统页面大小，默认为 4KB。
    public static final int OS_PAGE_SIZE = 1024 * 4;

    // 统计映射的虚拟内存总大小。
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    // 统计已映射的文件数。
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);


    // 当前文件的写指针，从0开始（内存映射文件中的写指针），即已写入到MappedFile中的数据位置。
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    // 当前文件的提交指针，如果开启transientStore-PoolEnable，则数据会存储在TransientStorePool中，然后提交到内存映射ByteBuffer中，再写入磁盘。
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    // 将该指针之前的数据持久化存储到磁盘中。（已刷盘的数据位置）
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    // 文件大小
    protected int fileSize;
    // 文件通道
    protected FileChannel fileChannel;

    /**
     * 当写入数据超过页面大小时，写入该缓冲区。
     * <p>
     * 堆外内存ByteBuffer，如果不为空，数据首先将存储在该Buffer中，然后提交到MappedFile创建的FileChannel中。
     * transientStorePoolEnable为true时不为空。
     * <p>
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;

    // 短暂存储池，用于提高数据写入性能。
    protected TransientStorePool transientStorePool = null;

    // 文件名称
    private String fileName;

    // 文件起始偏移量。
    private long fileFromOffset;
    // 文件对象。
    private File file;
    // 内存映射的ByteBuffer。（物理文件对应的内存映射Buffer）
    private MappedByteBuffer mappedByteBuffer;

    // 最后一次更新MappedFile时间戳
    private volatile long storeTimestamp = 0;
    // 是否是队列中第一个MappedFile。
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    /**
     * 确定目录路径是否OK
     *
     * @param dirName 目录名称
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                // 不存在则创建
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null) return buffer;
        else return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 初始化
     *
     * @param fileName           文件名称
     * @param fileSize           文件大小
     * @param transientStorePool 短暂存储池（堆外内存）
     */
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        // 初始化
        init(fileName, fileSize);

        // 借用缓冲区
        this.writeBuffer = transientStorePool.borrowBuffer();
        // 短暂存储池
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        // 文件名称、文件大小
        this.fileName = fileName;
        this.fileSize = fileSize;
        // 对应文件语柄
        this.file = new File(fileName);
        // 文件起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        // 确保目录正常
        ensureDirOK(this.file.getParent());

        try {
            // 创建读写文件通道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 核心：将文件内容使用 NIO的内存映射Buffer 将文件映射到内存中 ！！！
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            // 更新 统计映射的虚拟内存总大小
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            // 更新 统计已映射的文件数
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            // 如果失败，则关闭 文件通道。
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }


    /**
     * 方法是将消息数据追加到当前 MappedFile 对象所对应的内存映射文件中，并返回追加结果。
     *
     * @param msg 消息
     * @param cb  回调
     * @return AppendMessageResult
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 方法是将消息数据追加到当前 MappedFile 对象所对应的内存映射文件中，并返回追加结果。
     * <p>
     * 这个方法并没有直接将消息内容写入到磁盘，而是将消息内容先写入到内存之中，具体来说，是先将消息内容写入到了内存映射文件的写缓冲区中，
     * 然后再将写缓冲区中的内容刷入内存映射文件对应的物理文件中。
     * <p>
     * 写缓冲区是内存映射文件的一部分，它的作用是为了减少频繁的 I/O 操作而引入的。
     * 当调用 MappedFile#flush 方法时，写缓冲区的内容将被刷入磁盘中。
     * 在上述代码中，如果写缓冲区存在，则使用写缓冲区的内容进行消息写入操作，否则使用内存映射文件的内容进行操作。
     * 最后，wrotePosition 和 storeTimestamp 属性会被更新为写入的字节数和存储时间戳，以供后续使用。
     *
     * @param messageExt 消息
     * @param cb         回调
     * @return AppendMessageResult
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {

        // 校验参数
        assert messageExt != null;
        assert cb != null;

        // 获取当前写入位置
        int currentPos = this.wrotePosition.get();

        // 如果当前写入位置小于文件大小，说明可以继续写入消息
        if (currentPos < this.fileSize) {

            /*
             * TODO：核心：如何将消息追加或写入到 缓冲区中 ？
             *
             * 假设有对象 a，a 对象是通过 fileChannel.map 创建的 MappedByteBuffer 对象。
             * 对 a.slice() 方法返回的新的 MappedByteBuffer 对象 b 中添加数据，然后调用 a.force 刷盘， 在 b 中写入的数据，也会持久化到磁盘吗 ?
             *
             * ChatGPT: 是的，通过 a.slice() 方法返回的新的 MappedByteBuffer 对象 b 是与 a 对象共享同一块物理内存区域的，
             * 因此当你在 b 中添加数据后，通过 a.force() 方法将 a 对象的数据刷盘，b 中的数据也会被持久化到磁盘中。
             * 这是因为内存映射文件技术的原理是将文件映射到内存中，操作内存就相当于操作文件，所以对 MappedByteBuffer 的操作都会被直接映射到对应的文件中
             *
             *
             * ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
             *
             * byteBuffer 对象就是 mmap 映射文件(mappedByteBuffer)的一部分 内存对象。 向 byteBuffer 对象中添加数据，即是向内存中添加数据。
             * 在刷盘时调用 映射文件(mappedByteBuffer)的 force 方法时，会将映射内存区域的所有数据 刷到磁盘！！！
             *
             * 所以，byteBuffer 需要设置 position，并且 MappedFile 需要维护 wrotePosition ！！
             *
             */

            // 获取写入位置之后的字节缓冲区
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);

            AppendMessageResult result;

            // 判断消息类型，并调用对应的回调函数进行追加操作
            if (messageExt instanceof MessageExtBrokerInner) {

                // 核心处理：单个消息追加 ！！
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {

                // 批量消息追加
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            // 更新当前写入位置，
            this.wrotePosition.addAndGet(result.getWroteBytes());
            // 更新 最后一次更新MappedFile时间戳
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        // 如果当前写入位置已经达到文件大小，则返回追加失败结果
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:" + this.getFlushedPosition() + ", " + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
