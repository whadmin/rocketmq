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

import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 在MessageStoreConfig#enableConsumeQueueExt==true时生效
 * 消费队列的扩展属性数据，存储在MappedFileQueue文件队列中
 * 默认路径 {user.home}/store/consumequeue_ext/{topic}/{queueId}/
 * MappedFile 文件存储数据结构为CqExtUnit
 */
public class ConsumeQueueExt {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * consumeQueueExt文件队列
     */
    private final MappedFileQueue mappedFileQueue;

    /**
     * ConsumeQueueExt 从属于topic
     */
    private final String topic;

    /**
     * ConsumeQueueExt 从属于queueId
     */
    private final int queueId;

    /**
     * consumeQueueExt存储路径，
     * 默认 user.home/store/consumequeue_ext
     */
    private final String storePath;

    /**
     * consumeQueueExt文件队列中文件mappedFile大小，默认48M
     */
    private final int mappedFileSize;

    /**
     * 临时存储数据字节缓冲区
     */
    private ByteBuffer tempContainer;

    /**
     * consumeQueueExt文件结尾需要预留4字节空白
     */
    public static final int END_BLANK_DATA_LENGTH = 4;


    public static final long MAX_ADDR = Integer.MIN_VALUE - 1L;
    public static final long MAX_REAL_OFFSET = MAX_ADDR - Long.MIN_VALUE;

    /**
     * 构造
     * @param topic topic
     * @param queueId id of queue
     * @param storePath root dir of files to store.
     * @param mappedFileSize file size
     * @param bitMapLength bit map length.
     */
    public ConsumeQueueExt(final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final int bitMapLength) {

        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        if (bitMapLength > 0) {
            this.tempContainer = ByteBuffer.allocate(
                bitMapLength / Byte.SIZE
            );
        }
    }

    /**
     * 检查{@code address}cqExtUnit偏移坐标是否小于MAX_ADDR（-2147483649）
     * 用于判断式否需要进行编码解码
     */
    public static boolean isExtAddr(final long address) {
        return address <= MAX_ADDR;
    }

    /**
     * 对存储cqExtUnit偏移坐标进行解码
     */
    public long unDecorate(final long address) {
        //如果存储cqExtUnit编码后偏移坐标<=MAX_ADDR（-2147483649）进行编码
        if (isExtAddr(address)) {
            return address - Long.MIN_VALUE;
        }
        return address;
    }

    /**
     * 对存储cqExtUnit偏移坐标进行编码
     */
    public long decorate(final long offset) {
        //如果存储cqExtUnit偏移坐标>MAX_ADDR（-2147483649）进行编码
        if (!isExtAddr(offset)) {
            return offset + Long.MIN_VALUE;
        }
        return offset;
    }

    /**
     * 获取ConsumeQueueExt文件队列address偏移坐标开始后第一条CqExtUnit数据
     * @param address less than 0
     */
    public CqExtUnit get(final long address) {
        //创建CqExtUnit
        CqExtUnit cqExtUnit = new CqExtUnit();
        //获取ConsumeQueueExt中address偏移坐标后存储的CqExtUnit数据
        if (get(address, cqExtUnit)) {
            return cqExtUnit;
        }

        return null;
    }

    /**
     * 获取ConsumeQueueExt文件队列address偏移坐标开始后第一条CqExtUnit数据
     * @param address less than 0
     */
    public boolean get(final long address, final CqExtUnit cqExtUnit) {
        if (!isExtAddr(address)) {
            return false;
        }

        //获取consumeQueueExt文件队列mappedFile文件大小
        final int mappedFileSize = this.mappedFileSize;
        //对address偏移坐标解码
        final long realOffset = unDecorate(address);
        //获取consumeQueueExt文件队列中realOffset偏移所属的MappedFile对象
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(realOffset, realOffset == 0);
        if (mappedFile == null) {
            return false;
        }

        //读取文件MappedFile对应字节缓冲区pos偏移坐标开始的字节数据
        int pos = (int) (realOffset % mappedFileSize);
        SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos);
        if (bufferResult == null) {
            log.warn("[BUG] Consume queue extend unit({}) is not found!", realOffset);
            return false;
        }
        boolean ret = false;
        try {
            //读取bufferResult.getByteBuffer()字节缓冲区中数据设置到CqExtUnit对应属性中
            ret = cqExtUnit.read(bufferResult.getByteBuffer());
        } finally {
            bufferResult.release();
        }

        return ret;
    }

    /**
     * 将cqExtUnit数据写入ConsumeQueueExt文件队列
     */
    public long put(final CqExtUnit cqExtUnit) {
        //重试次数
        final int retryTimes = 3;
        try {
            //获取cqExtUnit数据结构大小，校验大小大于MAX_EXT_UNIT_SIZE
            int size = cqExtUnit.calcUnitSize();
            if (size > CqExtUnit.MAX_EXT_UNIT_SIZE) {
                log.error("Size of cq ext unit is greater than {}, {}", CqExtUnit.MAX_EXT_UNIT_SIZE, cqExtUnit);
                return 1;
            }
            //校验ConsumeQueueExt文件队列MappedFileQueue中最后一个MappedFile追加cqExtUnit是否大于MAX_REAL_OFFSET
            if (this.mappedFileQueue.getMaxOffset() + size > MAX_REAL_OFFSET) {
                log.warn("Capacity of ext is maximum!{}, {}", this.mappedFileQueue.getMaxOffset(), size);
                return 1;
            }
            //创建一个临时字节缓冲区
            if (this.tempContainer == null || this.tempContainer.capacity() < size) {
                this.tempContainer = ByteBuffer.allocate(size);
            }

            //重试3次
            for (int i = 0; i < retryTimes; i++) {
                //返回ConsumeQueueExt文件队列中最后一个mappedFile
                MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

                //校验mappedFile是否写满，如果已经写满创建出新的mappedFile
                if (mappedFile == null || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                }

                //校验异常
                if (mappedFile == null) {
                    log.error("Create mapped file when save consume queue extend, {}", cqExtUnit);
                    continue;
                }

                //获取文件可以写入的字节数据
                final int wrotePosition = mappedFile.getWrotePosition();
                final int blankSize = this.mappedFileSize - wrotePosition - END_BLANK_DATA_LENGTH;

                // 校验当前是否能满足写入cqExtUnit，.
                if (size > blankSize) {
                    fullFillToEnd(mappedFile, wrotePosition);
                    log.info("No enough space(need:{}, has:{}) of file {}, so fill to end",
                        size, blankSize, mappedFile.getFileName());
                    continue;
                }

                //cqExtUnit数据写入mappedFile文件中
                if (mappedFile.appendMessage(cqExtUnit.write(this.tempContainer), 0, size)) {
                    //返回写入cqExtUnit数据写入的偏移坐标
                    return decorate(wrotePosition + mappedFile.getFileFromOffset());
                }
            }
        } catch (Throwable e) {
            log.error("Save consume queue extend error, " + cqExtUnit, e);
        }

        return 1;
    }

    /**
     * 设置mappedFile 文件对应字节缓冲区坐标到完结
     * @param mappedFile
     * @param wrotePosition
     */
    protected void fullFillToEnd(final MappedFile mappedFile, final int wrotePosition) {
        ByteBuffer mappedFileBuffer = mappedFile.sliceByteBuffer();
        mappedFileBuffer.position(wrotePosition);

        // ending.
        mappedFileBuffer.putShort((short) -1);

        mappedFile.setWrotePosition(this.mappedFileSize);
    }

    /**
     * 加载ConsumeQueueExt文件队列
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue extend" + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * 校验ConsumeQueueExt文件队列文件大小
     */
    public void checkSelf() {
        this.mappedFileQueue.checkSelf();
    }

    /**
     * recover方法，是用来检测清理ConsumeQueueExt队列文件脏文件 流程参考ConsumeQueue
     */
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (mappedFiles == null || mappedFiles.isEmpty()) {
            return;
        }

        // load all files, consume queue will truncate extend files.
        int index = 0;

        MappedFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        long processOffset = mappedFile.getFileFromOffset();
        long mappedFileOffset = 0;
        CqExtUnit extUnit = new CqExtUnit();

        while (true) {
            extUnit.readBySkip(byteBuffer);

            // check whether write sth.
            if (extUnit.getSize() > 0) {
                mappedFileOffset += extUnit.getSize();
                continue;
            }

            index++;
            if (index < mappedFiles.size()) {
                mappedFile = mappedFiles.get(index);
                byteBuffer = mappedFile.sliceByteBuffer();
                processOffset = mappedFile.getFileFromOffset();
                mappedFileOffset = 0;
                log.info("Recover next consume queue extend file, " + mappedFile.getFileName());
                continue;
            }

            log.info("All files of consume queue extend has been recovered over, last mapped file "
                + mappedFile.getFileName());
            break;
        }

        processOffset += mappedFileOffset;
        this.mappedFileQueue.setFlushedWhere(processOffset);
        this.mappedFileQueue.setCommittedWhere(processOffset);
        this.mappedFileQueue.truncateDirtyFiles(processOffset);
    }


    /**
     *  清理掉ConsumeQueueExt文件队列中 偏移坐标小于【minAddress】所有历史mappedFile文件
     *
     * @param minAddress less than 0
     */
    public void truncateByMinAddress(final long minAddress) {
        if (!isExtAddr(minAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by min {}.", minAddress);

        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        final long realOffset = unDecorate(minAddress);

        for (MappedFile file : mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;

            if (fileTailOffset < realOffset) {
                log.info("Destroy consume queue ext by min: file={}, fileTailOffset={}, minOffset={}", file.getFileName(),
                    fileTailOffset, realOffset);
                if (file.destroy(1000)) {
                    willRemoveFiles.add(file);
                }
            }
        }

        this.mappedFileQueue.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 清理掉ConsumeQueueExt文件队列中 偏移坐标大于【maxAddress偏移+其后一条cqExtUnit.getSize()】所有脏mappedFile文件
     * @param maxAddress less than 0
     */
    public void truncateByMaxAddress(final long maxAddress) {
        if (!isExtAddr(maxAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by max {}.", maxAddress);

        CqExtUnit cqExtUnit = get(maxAddress);
        if (cqExtUnit == null) {
            log.error("[BUG] address {} of consume queue extend not found!", maxAddress);
            return;
        }

        final long realOffset = unDecorate(maxAddress);

        this.mappedFileQueue.truncateDirtyFiles(realOffset + cqExtUnit.getSize());
    }

    /**
     * 将写入到fileChannel的数据数据刷写到磁盘
     */
    public boolean flush(final int flushLeastPages) {
        return this.mappedFileQueue.flush(flushLeastPages);
    }

    /**
     * 删除ConsumeQueueExt队列文件
     */
    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * 返回ConsumeQueueExt队列文件写入最后一条CqExtUnit数据偏移坐标
     */
    public long getMaxAddress() {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile == null) {
            return decorate(0);
        }
        return decorate(mappedFile.getFileFromOffset() + mappedFile.getWrotePosition());
    }

    /**
     * 返回ConsumeQueueExt队列文件第一个mappedFile偏移坐标
     */
    public long getMinAddress() {
        MappedFile firstFile = this.mappedFileQueue.getFirstMappedFile();
        if (firstFile == null) {
            return decorate(0);
        }
        return decorate(firstFile.getFileFromOffset());
    }

    /**
     * 表述了ConsumeQueueExt管理下mappedFile里面存放的数据结构
     */
    public static class CqExtUnit {

        //表示CqExtUnit结构中除了filterBitMap之外，其他属性占用的总字节长度大小
        //size(short) + tagsCode(long) + msgStoreTime(long) + bitMapSize(short) = 20字节
        public static final short MIN_EXT_UNIT_SIZE
            = 2 * 1 // size, 32k max
            + 8 * 2 // msg time + tagCode
            + 2; // bitMapSize


        public static final int MAX_EXT_UNIT_SIZE = Short.MAX_VALUE;

        public CqExtUnit() {
        }

        public CqExtUnit(Long tagsCode, long msgStoreTime, byte[] filterBitMap) {
            this.tagsCode = tagsCode == null ? 0 : tagsCode;
            this.msgStoreTime = msgStoreTime;
            this.filterBitMap = filterBitMap;
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);
        }

        /**
         * CqExtUnit 结构占用的总大小 MIN_EXT_UNIT_SIZE + this.bitMapSize
         */
        private short size;
        /**
         * tagsCode
         */
        private long tagsCode;
        /**
         * CqExtUnit存储时间
         */
        private long msgStoreTime;
        /**
         * bitMapSize数据长度
         */
        private short bitMapSize;
        /**
         * filterBitMap数据
         */
        private byte[] filterBitMap;

        /**
         * 读取yteBuffer字节缓冲区中数据设置到CqExtUnit对应属性中
         */
        private boolean read(final ByteBuffer buffer) {
            //校验buffer数据长度需要大于2个字节
            if (buffer.position() + 2 > buffer.limit()) {
                return false;
            }

            //获取CqExtUnit总大小，并校验
            this.size = buffer.getShort();
            if (this.size < 1) {
                return false;
            }

            //设置tagsCode msgStoreTime bitMapSize
            this.tagsCode = buffer.getLong();
            this.msgStoreTime = buffer.getLong();
            this.bitMapSize = buffer.getShort();

            //校验bitMapSize,如果小于说明不存在filterBitMap直接返回
            if (this.bitMapSize < 1) {
                return true;
            }

            //设置filterBitMap
            if (this.filterBitMap == null || this.filterBitMap.length != this.bitMapSize) {
                this.filterBitMap = new byte[bitMapSize];
            }
            buffer.get(this.filterBitMap);
            return true;
        }

        /**
         * 将MappedFile position坐标指向下条CqExtUnit初始位置
         */
        private void readBySkip(final ByteBuffer buffer) {
            ByteBuffer temp = buffer.slice();

            short tempSize = temp.getShort();
            this.size = tempSize;

            if (tempSize > 0) {
                buffer.position(buffer.position() + this.size);
            }
        }

        /**
         * 读取CqExtUnit对应属性写入到ByteBuffer字节缓冲区中，返回字节数组
         */
        private byte[] write(final ByteBuffer container) {
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);

            ByteBuffer temp = container;

            if (temp == null || temp.capacity() < this.size) {
                temp = ByteBuffer.allocate(this.size);
            }

            temp.flip();
            temp.limit(this.size);

            temp.putShort(this.size);
            temp.putLong(this.tagsCode);
            temp.putLong(this.msgStoreTime);
            temp.putShort(this.bitMapSize);
            if (this.bitMapSize > 0) {
                temp.put(this.filterBitMap);
            }

            return temp.array();
        }

        /**
         * Calculate unit size by current data.
         */
        private int calcUnitSize() {
            int sizeTemp = MIN_EXT_UNIT_SIZE + (filterBitMap == null ? 0 : filterBitMap.length);
            return sizeTemp;
        }

        public long getTagsCode() {
            return tagsCode;
        }

        public void setTagsCode(final long tagsCode) {
            this.tagsCode = tagsCode;
        }

        public long getMsgStoreTime() {
            return msgStoreTime;
        }

        public void setMsgStoreTime(final long msgStoreTime) {
            this.msgStoreTime = msgStoreTime;
        }

        public byte[] getFilterBitMap() {
            if (this.bitMapSize < 1) {
                return null;
            }
            return filterBitMap;
        }

        public void setFilterBitMap(final byte[] filterBitMap) {
            this.filterBitMap = filterBitMap;
            // not safe transform, but size will be calculate by #calcUnitSize
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
        }

        public short getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof CqExtUnit))
                return false;

            CqExtUnit cqExtUnit = (CqExtUnit) o;

            if (bitMapSize != cqExtUnit.bitMapSize)
                return false;
            if (msgStoreTime != cqExtUnit.msgStoreTime)
                return false;
            if (size != cqExtUnit.size)
                return false;
            if (tagsCode != cqExtUnit.tagsCode)
                return false;
            if (!Arrays.equals(filterBitMap, cqExtUnit.filterBitMap))
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) size;
            result = 31 * result + (int) (tagsCode ^ (tagsCode >>> 32));
            result = 31 * result + (int) (msgStoreTime ^ (msgStoreTime >>> 32));
            result = 31 * result + (int) bitMapSize;
            result = 31 * result + (filterBitMap != null ? Arrays.hashCode(filterBitMap) : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CqExtUnit{" +
                "size=" + size +
                ", tagsCode=" + tagsCode +
                ", msgStoreTime=" + msgStoreTime +
                ", bitMapSize=" + bitMapSize +
                ", filterBitMap=" + Arrays.toString(filterBitMap) +
                '}';
        }
    }
}
