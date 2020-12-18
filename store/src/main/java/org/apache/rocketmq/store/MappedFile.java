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
 * MappedFile 用来表示mq 文件系统中 文件对象
 * MappedFile 可以用来操作commitlog目录下消息文件
 * MappedFile 可以用来操作consumequeue目录下消费队列文件
 * MappedFile 可以用来操作index 目录下索引文件
 * <p>
 * <p>
 * MappedFile 存在2种构造方法，不同的构造方法在调用 appendMessagesInner 写入消息数据时会采用不同的策略
 * public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb)
 * <p>
 * 不使用TransientStorePool
 * <p>
 * 1 消息文件会写入mappedByteBuffer内存映射字节缓冲区中 记录committedPosition
 * 2 通过brokerFlush线程定时调用 flush 方法将mappedByteBuffer内存映射字节缓冲区中写入磁盘 记录flushedPosition
 * <p>
 * 使用TransientStorePool
 * 1 初始化的时候会从TransientStorePool（堆外内存池） 获得一个writeBuffer（堆外内存缓存区）
 * 2 消息文件会写入writeBuffer（堆外内存缓存区）
 * 3 通过Commit线程定时调用 commit 方法将writeBuffer（堆外内存缓存区）数据写入fileChannel，记录committedPosition
 * 4 通过brokerFlush线程定时调用 flush 方法将将fileChannel中数据写入磁盘，记录flushedPosition
 */
public class MappedFile extends ReferenceResource {

    /**
     * 日志文件
     */
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 内存页大小，4KB
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    /**
     * 记录commitlog目录下所有MappedFile占用内存的总大小
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 记录commitlog目录下所有MappedFile总文件数
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * MappedFile文件对应mappedByteBuffer（内存映射字节缓冲区）
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * MappedFile文件对应fileChannel
     */
    protected FileChannel fileChannel;

    /**
     * 堆外内存池
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 通过transientStorePool获取ByteBuffer（堆外内存字节缓冲区）
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * wrotePosition mappedByteBuffer/writeBuffer 字节缓冲区中pos写入位置
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * MappedFile使用TransientStorePool时
     * broker commit 线程定时调用 commit 方法将writeBuffer（堆外内存缓存区）数据写入fileChannel
     * committedPosition 表示fileChannel.commit后内存写入位置。理论上==wrotePosition
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * broker flush 线程定时调用 flush 方法将mappedByteBuffer/fileChannel写入磁盘 记录flushedPosition
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);


    /**
     * MappedFile对应物理文件对象
     */
    private File file;

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
     * 最后一次追加消息写入ByteBuffer的时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 是否是MappedFileQueue队列中第一个MappedFile
     */
    private boolean firstCreateInQueue = false;

    /**
     * 创建MappedFile
     */
    public MappedFile() {
    }

    /**
     * 创建MappedFile并设置文件名称，大小
     * 使用当前方法创建的MappedFile并没有使用 TransientStorePool（堆外内存池）
     * 1 消息文件会写入mappedByteBuffer内存映射字节缓冲区中 记录committedPosition
     * 2 通过brokerFlush线程定时调用 flush 方法将mappedByteBuffer内存映射字节缓冲区中写入磁盘 记录flushedPosition
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 创建MappedFile并设置文件名称，大小，同时设置TransientStorePool（堆外内存池）
     * 1 初始化的时候会从TransientStorePool（堆外内存池） 获得一个writeBuffer（堆外内存缓存区）
     * 2 消息文件会写入writeBuffer（堆外内存缓存区）
     * 3 通过Commit线程定时调用 commit 方法将writeBuffer（堆外内存缓存区）数据写入fileChannel，记录committedPosition
     * 4 通过brokerFlush线程定时调用 flush 方法将将fileChannel中数据写入磁盘，记录flushedPosition
     */
    public MappedFile(final String fileName, final int fileSize,
                      final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }


    /***********************    初始化 star       ***********************/
    /**
     * 初始化MappedFile,使用transientStorePool(堆外内存池)
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
    /***********************    初始化 end       ***********************/

    /***********************    追加数据 star       ***********************/
    /**
     * 向MapperFile追加消息,消息会写入ByteBuffer（堆外内存字节缓冲区）/mappedByteBuffer（内存映射字节缓冲区），等待异步刷盘
     *
     * @param msg
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    /**
     * 向MapperFile追加批量消息，,消息会写入ByteBuffer（堆外内存字节缓冲区）/mappedByteBuffer（内存映射字节缓冲区），等待异步刷盘
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
            //通过判断writeBuffer（推外内存字节缓冲区）来判断是否使用堆外内存池
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
     * MappedFile 文件尾部追加数据（直接写入文件）
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
     * MappedFile 文件指定偏移位置追加数据（直接写入文件）
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
     * 将mappedByteBuffer或fileChannel中数据写入磁盘，
     *
     * @param flushLeastPages  用来判断是否可以刷新最小页
     * @return flushedPosition 刷新偏移
     */
    public int flush(final int flushLeastPages) {
        //判断是否可以进行刷盘
        if (this.isAbleToFlush(flushLeastPages)) {
            //引用计数+1
            if (this.hold()) {
                // 获取可以写入磁盘的偏移（mappedByteBuffer对应wrotePosition，也可能是writeBuffer对应committedPosition）
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
                //记录flushedPosition=getReadPosition()
                this.flushedPosition.set(value);
                //引用计数-1
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
     * @param flushLeastPages flushLeastPages > 0 只有write-flush > flushLeastPages * 4K 才会刷盘
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
     * MappedFile使用transientStorePool追加数据时
     * ByteBuffer字节缓冲区中的数据写入fileChannel
     *
     * @param commitLeastPages  用来判断是否可以刷新最小页
     * @return flushedPosition  提交的偏移
     */
    public int commit(final int commitLeastPages) {
        //当writeBuffer为null，直接返回 wrotePosition
        if (writeBuffer == null) {
            return this.wrotePosition.get();
        }
        //是否可以提交
        if (this.isAbleToCommit(commitLeastPages)) {
            //引用计数+1
            if (this.hold()) {
                // 提交核心实现
                commit0(commitLeastPages);
                //引用计数-1
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }
        //如果文件写满了将writeBuffer堆外内存字节缓冲区 归还给 对外内存池
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            //将writeBuffer堆外内存字节缓冲区 归还给 对外内存池
            this.transientStorePool.returnBuffer(writeBuffer);
            //设置writeBuffer堆外内存字节缓冲区为null
            this.writeBuffer = null;
        }
        //返回提交的偏移
        return this.committedPosition.get();
    }

    /**
     * 将ByteBuffer字节缓冲区中的数据写入fileChannel
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
                //记录committedPosition
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
    /***********************    追加数据 end       ***********************/


    /***********************    选择获取MappedFile数据 star   ***********************/
    /**
     * 读取mappedFile文件 pos物理偏移坐标开始, size长度字节数据
     *
     * @param pos  mappedFile文件数据初始偏移坐标
     * @param size 长度
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
     * 读取mappedFile文件 pos物理偏移坐标开始所有字节数据
     *
     * @param pos mappedFile文件数据初始偏移坐标
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


    /**
     * 获取文件内存映射
     *
     * @return
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    /***********************    选择获取MappedFile数据 end   ***********************/


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


    /***********************    清理 star   ***********************/
    /**
     * 清理mappedFile
     *
     * @return
     */
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

    /***********************    清理 end   ***********************/


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
     *
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
