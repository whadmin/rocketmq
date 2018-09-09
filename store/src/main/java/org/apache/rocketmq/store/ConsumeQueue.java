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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 我们将消息坐标信息MESSAGE_POSITION_INFO，以固定长度20个字节（long+long+int）存储到ConsumeQueue队列文件队列中
 *
 * 有两种内容类型：
 * 1 MESSAGE_POSITION_INFO ：消息位置信息。
 *
 * 第几位 	字段 	   说明 	                    数据类型 	    字节数
 * 1 	   offset 	   消息 CommitLog 偏移坐标 	Long 	      8
 * 2 	   size 	   消息长度 	                Int 	      4
 * 3 	   tagsCode    消息tagsCode 	            Long 	      8
 *
 * 2 BLANK : MappedFile文件前置空白占位。当历史 Message 被删除时，需要用 BLANK占位被删除的消息。
 *
 * 第几位 	字段 	   说明                      数据类型 	字节数
 * 1 		0 	                                Long 	    8
 * 2 		Integer.MAX_VALUE 	                Int 	    4
 * 3 		0 	                                Long 	    8
 *
 *  默认路径 {user.home}/store/consumequeue/{topic}/{queueId}/
 *
 *  defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt() 开启时
 *
 *  会将消息的ConsumeQueueExt
 *
 *
 */
public class ConsumeQueue {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 每条消息存储在ConsumeQueue固定长度大小
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;

    /**
     *  defaultMessageStore
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * ConsumeQueue 文件队列
     */
    private final MappedFileQueue mappedFileQueue;

    /**
     * ConsumeQueue 从属于topic
     */
    private final String topic;

    /**
     * ConsumeQueue 从属于queueId
     */
    private final int queueId;

    /**
     * 临时存储数据字节缓冲区【数据先写入临时缓冲区完毕，在写入文件fileChannel】
     */
    private final ByteBuffer byteBufferIndex;

    /**
     * ConsumeQueue 存储路径，
     */
    private final String storePath;

    /**
     * ConsumeQueue 文件队列中文件mappedFile大小
     */
    private final int mappedFileSize;

    /**
     * 记录消息最大CommitLog偏移坐标
     */
    private long maxPhysicOffset = -1;

    /**
     * 记录ConsumeQueue文件队列中最小CommitLog物理偏移坐标
     * 这里有人会问肯定是0啊，但是mq会定时不断清理掉过去文件，这时需要不断更新minLogicOffset
     */
    private volatile long minLogicOffset = 0;

    /**
     * ConsumeQueue 扩展数据
     */
    private ConsumeQueueExt consumeQueueExt = null;

    /**
     * 构造
     * @param topic
     * @param queueId
     * @param storePath
     * @param mappedFileSize
     * @param defaultMessageStore
     */
    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        //是否打开消息的ConsumeQueue扩展功能
        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    /**
     * 加载ConsumeQueue 文件队列
     * 打开消息的ConsumeQueue扩展功能 则加载 ConsumeQueueExt文件队列
     * @return
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * recover方法，是用来检测清理ComsumeQueue队列文件脏文件
     * 流程如下
     * 1 遍历读取ComsumeQueue队列文件最后三个MappedFile，所有坐标信息MESSAGE_POSITION_INFO,更新更新maxPhysicOffset以及maxExtAddr
     * 2 检查是否存在不是最后一个MappedFile，但是文件没有写满的MappedFile，并获取该mappedFile.getFileFromOffset()的偏移坐标
     * 3 清理mappedFile.getFileFromOffset()后续所有MappedFile，将该偏移坐标设置到  mappedFileQueue committedWhere committedWhere
     * 4 如果开启ext，清除maxExtAddr之后的记录
     */
    public void recover() {
        //获取文件队列中所有MappedFile
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            //获取倒数第三个MappedFile在集合List中的下标【】
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            //设置MappedFile文件大小
            int mappedFileSizeLogics = this.mappedFileSize;

            //依次遍历读取最后三个MappedFile
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();

            //记录MappedFile文件写入最大偏移
            long mappedFileOffset = 0;
            //记录MappedFile中最后一个坐标信息MESSAGE_POSITION_INFO对应tagsCode
            long maxExtAddr = 1;
            while (true) {
                //遍历MappedFile文件中每条消息坐标信息MESSAGE_POSITION_INFO，将CommitLog偏移坐标更新到ConsumeQueue.maxPhysicOffset
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxPhysicOffset = offset;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }
                //检查是否存在不是最后一个MappedFile，但是文件没有写满的MappedFile，并获取该位置的偏移坐标
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    //最后最后一个MappedFile，退出循环
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                }
                //没有写满，说明一定是最后一个MappedFile，退出循环
                else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }
            //processOffset偏移坐标坐标设置到mappedFileQueue committedWhere committedWhere
            //清理processOffset偏移坐标后的MappedFile文件
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            //如果开启ext，清除maxExtAddr之后的记录
            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    /**
     * 找到消息发送时间最接近timestamp逻辑队列的offset
     * @param timestamp
     * @return
     */
    public long getOffsetInQueueByTime(final long timestamp) {
        //获取最后修改时间在timestamp之后的第一个mappedFile
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            //?
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            //获取CommitLog文件队列中第一个状态生效的mappedFile偏移坐标
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            //读取ConsumeQueue队列文件MappedFile对应字节缓冲区
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime =
                            this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    /**
     * 删除phyOffet之后的脏文件(包括同mappedFile之后记录,通过修改wrotePosition保证) * 如果开启ext，找到maxExtAddr，把ext的脏文件也删除掉 * *
     * 1:依次处理最后一个，倒数第二个MappedFile。。。 *
     * 2.每个文件从第一个数据块开始解析，如果超过phyOffet就，就删掉该文件 *
     * 3.否则该文件继续遍历后续记录，不断修改wrote,commit和flushPosition,直到遍历结束或者数据块大小为空则返回 * 4.如果所有mappedFile都删完了，再truncateByMaxAddress
     * @param phyOffet
     */
    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        this.maxPhysicOffset = phyOffet - 1;
        long maxExtAddr = 1;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        if (offset >= phyOffet) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffet) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    /**
     * 获取consumeQueue 最后一条消息的CommitLog物理偏移坐标
     * @return
     */
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;
        // 返回consumeQueue文件队列mappedFiles中最后一个mappedFile
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {
            //减去前置空白占位，获取第一条位置信息的偏移坐标
            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            //获取缓冲区设置pos 偏移坐标
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    /**
     * 删除从ConsumeQueue队列文件中,清理掉消息ommitLog物理偏移小于offset数据
     * @param offset
     * @return
     */
    public int deleteExpiredFile(long offset) {
        //删除从ConsumeQueue队列文件中,清理掉消息ommitLog物理偏移小于offset数据
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * 清理触发后重置minExtAddr,minLogicOffset
     * @param phyMinOffset
     */
    public void correctMinOffset(long phyMinOffset) {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = result.getMappedFile().getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 消息调度请求中消息数据同步到ConsumeQueue文件队列中
     * */
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        //重试30次
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            //获取消息tag
            long tagsCode = request.getTagsCode();
            //是否开启ConsumeQueueExt
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());
                //消息TagsCode进入consumeQueueExt文件队列返回地址设置到tagsCode
                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            //将消息数据同步到ConsumeQueue文件队列中
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                //更新消息写入ConsumeQueue文件时间【这里只是写入字节缓冲区，没有刷写到磁盘】
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // 设置defaultMessageStore 执行状态发送发生写入ConsumeQueue文件队列异常
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    /**
     * 同步消息数据到ConsumeQueue队列文件
     * @param offset  消息的CommitLog物理偏移坐标
     * @param size    消息大小
     * @param tagsCode  消息TAG
     * @param cqOffset ConsumeQueue队列逻辑偏移坐标
     * @return
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        //校验offset
        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        // 写入位置信息到byteBuffer
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        // 计算消息consumeQueue队列文件存储位置，并获得consumeQueue队列文件中对应的MappedFile
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);

        if (mappedFile != null) {
            // 当是ConsumeQueue队列还没创建第一个MappedFile && 队列位置非第一个 && MappedFile未写入内容，则填充前置空白占位
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }
            // 校验consumeQueue存储位置是否合法。TODO 如果不合法，继续写入会不会有问题？
            if (cqOffset != 0) {
                //获取MappedFile文件是否对应字节缓存区写入位置
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();
                //校验
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }
                //校验
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            // 设置commitLog重放消息到ConsumeQueue位置。
            this.maxPhysicOffset = offset;
            // 插入mappedFile
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    /**
     * 填充前置空白占位
     * @param mappedFile
     * @param untilWhere
     */
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 获取索引坐标为startIndex的消息所在mappedFile的字节缓冲区分片ByteBuffer其中pos=startIndex * CQ_STORE_UNIT_SIZE% mappedFileSize
     * @param startIndex startIndex表示消息在ConsumeQueue文件队列中消息排序
     * @return
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        //存储在ConsumeQueue中队列消息长度固定，可以通过startIndex * CQ_STORE_UNIT_SIZE计算出消息偏移坐标
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            //通过offset在MappedFileQueue找到所属mappedFile
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                //读取文件MappedFile对应字节缓冲区pos偏移坐标开始的字节数据
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    /**
     * 获取ConsumeQueueExt文件队列offset偏移坐标qExtUnit数据
     * @param offset
     * @return
     */
    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    /**
     * 获取ConsumeQueueExt文件队列address偏移坐标CqExtUnit数据
     * @param offset
     * @param cqExtUnit
     * @return
     */
    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    /**
     * 获取索引坐标所在MappedFile的下一个MappedFile索引坐标
     * @param index
     * @return
     */
    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    /**
     * 清除ConsumeQueue，consumeQueueExt文件队列
     */
    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    /**
     * 获取ConsumeQueue队列文件写入消息的数量
     * @return
     */
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    /**
     * 获取ConsumeQueue写入消息最大索引坐标
     * @return
     */
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 检测ConsumeQueue，consumeQueueExt文件队列中文件大小
     */
    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    /**
     * 是否打开ConsumeQueueExt
     * @return
     */
    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    /**
     * 是否打开ConsumeQueueExt
     * @return
     */
    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} tagsCode 是否是否小于MAX_ADDR（-2147483649）
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
