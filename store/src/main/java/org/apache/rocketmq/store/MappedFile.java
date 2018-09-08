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
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

/**
 * MappedFile 用来表示文件队列中一个文件对象
 *
 * MappedFile 有2中类型方式去追加消息数据
 * <p>
 * 1 使用TransientStorePool
 * <p>
 * 流程如下
 * transientStorePool.borrowBuffer()从池中获取一块字节缓冲区---->appendMessagesInner写入ByteBuffer（记录wrotePosition）---->commit(将ByteBuffer数据写入fileChannel，记录committedPosition)
 * ---->flush(fileChannel.force()将fileChannel中数据写入磁盘，记录flushedPosition)
 * <p>
 * 2 不使用TransientStorePool
 * appendMessagesInner写入mappedByteBuffer（记录committedPosition）---->commit(什么也不做)---->flush(mappedByteBuffer.force()将mappedByteBuffer中数据写入磁盘，记录flushedPosition)
 */
public class MappedFile extends ReferenceResource {

    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * commitlog目录下所有MappedFile占用内存的总大小
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * commitlog目录下所有MappedFile总文件数
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * MappedFile文件对应的内存映射ByteBuffer
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * wrotePosition 表示writeBuffer或mappedByteBuffer字节缓冲区中pos写入位置
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * MappedFile使用TransientStorePool,会从池中获取一块字节缓冲区writeBuffer,追加数据不在写入mappedByteBuffer,
     * 而是写入writeBuffer,在调用执行commit时会将writeBuffer写入文件对应fileChannel。
     * committedPosition 表示fileChannel.commit后内存写入位置。理论上==wrotePosition
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * 使用TransientStorePool从transientStorePool.borrowBuffer()从池中获取一块字节缓冲区,写入消息会优先写入writeBuffer,
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * MappedFile文件对应fileChannel
     * 如果使用TransientStorePool，在调用commit方法中将writeBuffer写入fileChannel
     */
    protected FileChannel fileChannel;

    /**
     * ByteBuffer池对象
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 刷盘位置
     * 无论是否使用TransientStorePool，在调用flush方法会将fileChannel或mappedByteBuffer内存中的数据写入磁盘
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     * mappedFile文件大小，参照MessageStoreConfig.mapedFileSizeCommitLog，默认1G
     */
    protected int fileSize;

    /**
     * fileFromOffset 表示MappedFile文件名称是一个12位的数值，同时也表示文件存储的初始偏移
     * 00000000000000000000   初始偏移是 0   存储 0~ fileSize
     * 00000000001073741824   初始偏移是 1073741824   存储 fileSize~fileSize*2
     */
    private long fileFromOffset;

    /**
     * MappedFile文件名 于fileFromOffset一致
     */
    private String fileName;

    /**
     * MappedFile对应物理文件对象
     */
    private File file;

    /**
     * 最后一次追加消息写入ByteBuffer的时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 是否是MappedFileQueue队列中第一个MappedFile
     */
    private boolean firstCreateInQueue = false;

    /**
     * 构建MappedFile
     */
    public MappedFile() {
    }

    /**
     * 构建MappedFile,初始化文件名，大小
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 构建MappedFile,初始化文件名，大小，ByteBuffer池对象
     */
    public MappedFile(final String fileName, final int fileSize,
                      final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }


    /***********************    初始化函数 star       ***********************/
    /**
     * 初始化MappedFile,使用transientStorePool
     */
    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 初始化MappedFile
     * fileName 文件名，
     * fileSize 大小
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        //设置文件名称
        this.fileName = fileName;
        //设置文件大小
        this.fileSize = fileSize;
        //设置文件对象
        this.file = new File(fileName);
        //设置文件记录数据的偏移
        this.fileFromOffset = Long.parseLong(this.file.getName());
        //设置是否初始化成功
        boolean ok = false;

        //指定目录是否存在，不存在则创建一个
        ensureDirOK(this.file.getParent());

        try {
            //设置fileChannel
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            //设置mappedByteBuffer
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            //累计当前broker MappedFile总大小
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            //累计当前broker MappedFile个数
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    /**
     * 指定目录是否存在，不存在则创建一个
     *
     * @param dirName
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }
    /***********************    初始化函数 end       ***********************/

    /***********************    追加消息函数实现 star       ***********************/
    /**
     * 追加消息
     *
     * @param msg
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    /**
     * 追加批量消息
     *
     * @param messageExtBatch
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 追加消息核心实现
     *
     * @param messageExt
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        //获取文件写入的偏移位置
        int currentPos = this.wrotePosition.get();
        //文件还有剩余空间
        if (currentPos < this.fileSize) {
            //创建一个writeBuffer或mappedByteBufferd position 到 limit 的分片字节缓存区,
            //这里需要新缓冲区与原来的缓冲区的一部分共享数据，但两个缓冲区的位置，极限和标记值将是独立的，怎么理解？
            //我们通过向分片缓存区slice() 追加数据,因为数据共享此时mappedByteBuffer数据已经追加，但是mappedByteBuffer中的标记始终保持不变[pos=0 lim=10485760 cap=10485760]
            //所以我们每次slice()分片缓存区也都是[pos=0 lim=10485760 cap=10485760]，并自己记录pos，并在每次添加时手动设置。
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);

            //追加消息到byteBuffer中
            AppendMessageResult result = null;
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            //更新wrotePosition
            this.wrotePosition.addAndGet(result.getWroteBytes());
            //更新storeTimestamp
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }


    /**
     * MappedFile追加数据实现
     *
     * @param data
     * @return
     */
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
     * MappedFile追加数据实现
     *
     * @param data
     * @param offset 偏移位置
     * @param length 长度
     * @return
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
     * 将mappedByteBuffer或fileChannel中数据写入磁盘
     * 记录flushedPosition=getReadPosition()
     * @return flushedPosition
     */
    public int flush(final int flushLeastPages) {
        //是否可以刷盘
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //我们只将数据附加到fileChannel或mappedByteBuffer，而不是两者。
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        //将此缓冲区内容的任何更改写入包含映射文件的存储设备
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

    /**
     * 是否可以刷盘
     *
     * @param flushLeastPages
     * @return
     */
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


    /**
     * MappedFile使用transientStorePool追加消息时,
     * 需要调用执行commit将ByteBuffer字节缓冲区中的数据写入fileChannel
     * @param commitLeastPages
     * @return
     */
    public int commit(final int commitLeastPages) {
        //当writeBuffer为null，直接返回 wrotePosition
        if (writeBuffer == null) {
            return this.wrotePosition.get();
        }
        //是否可以提交
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
            //交还给transientStorePool
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * 将writeBuffer中[lastCommittedPosition,writePos)的部分 写入fileChannel
     * 更新committedPosition
     * 参数 commitLeastPages 没用
     *
     * @param commitLeastPages
     */
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                /**
                 * writeBuffer 写入和mappedByteBuffer写入一样都是使用分片字节缓冲区。因为坐标指针都是延时的（数据已经写入），
                 * 需要手动坐标
                 */
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

    /**
     * 是否可以提交
     *
     * @param commitLeastPages
     * @return
     */
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
    /***********************    追加消息函数 end       ***********************/


    /**
     * 返回从pos到 pos + size的内存映射
     *
     * @param pos
     * @param size
     * @return
     */
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
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     *
     * 读取文件MappedFile对应字节缓冲区pos偏移坐标开始的字节数据
     * @param pos
     * @return
     */
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
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
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
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }


    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }


    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        /**
         * 如果是directByteBuffer且容量不为0
         * 则嵌套拿到directByteBuffer的最内部的attachment，强制转换成ByteBuffer对象(实际运行应该会是directByteBuffer)，调用其clearner.clean方法进行深度的释放资源
         */
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

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    /**
     * 嵌套调用，获取最深层的attachment 或者 viewedBuffer方法
     * 转化为ByteBuffer对象
     */
    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }
        //执行DirectByteBuffer.attachment()方法
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    /**
     * 销毁 MappedFile
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    File getFile() {
        return this.file;
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
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

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
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

    /**
     * 返回MappedFile对应字节缓冲区区Pos偏移（可能是mappedByteBuffer对应wrotePosition，也可能是writeBuffer对应committedPosition）
     * @return
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
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

    @Override
    public String toString() {
        return this.fileName;
    }
}
