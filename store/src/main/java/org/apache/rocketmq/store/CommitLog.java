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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQ将消息数据信息以固定结构 存储到ConsumeQueue队列文件队列中
 *
 * 第几位 	数据类型 	   说明
     1 	    int 	 TOTALSIZE 	消息+元数据总长度
     2 	    int 	MAGICCODE 	魔数，固定值
     3 	    int 	BODYCRC 	消息crc
     4 	    int 	QUEUEID 	队列id
     5 	    int 	FLAG 	flag
     6 	    long 	QUEUEOFFSET 	队列偏移量
     7 	    long 	PHYSICALOFFSET 	物理偏移量
     8 	    long 	SYSFLAG 	sysflag
     9 	    long 	BORNTIMESTAMP 	消息产生时间
     10 	long 	BORNHOST 	消息产生的ip + port
     11 	long 	STORETIMESTAMP 	消息存储时间
     12 	long 	STOREHOSTADDRESS 	消息存储的ip + port
     13 	long 	RECONSUMETIMES 	重新消费的次数
     14 	long 	Prepared Transaction Offset 	事物相关偏移量
     15 	int 	BODY Length 	消息体长度
     ? 	    body 	消息体
     16 	byte 	topic length 	topic长度
     ？ 	    topic 	主题
     17 	short 	PROPERTIES LENGTH 	属性长度
     ？ 	    PROPERTIES 	属性
 *
 * 默认路径 {user.home}/store/CommitLog/
 */
public class CommitLog {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 消息的MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;
    // 文件结束空MAGIC CODE cbd43194
    private final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;

    private final DefaultMessageStore defaultMessageStore;

    /**
     * mappedFile队列
     */
    private final MappedFileQueue mappedFileQueue;


    /**
     * 刷写磁盘服务 【getMessageStoreConfig().isTransientStorePoolEnable()=false时默认的实现】
     * 会依据getMessageStoreConfig().getFlushDiskType()配置,使用同步或异步2中实现
     * 同步：GroupCommitService
     * 异步：FlushRealTimeService
     */
    private final FlushCommitLogService flushCommitLogService;


    /**
     * 刷写磁盘服务，【getMessageStoreConfig().isTransientStorePoolEnable()=true使用TransientStorePool的实现】
     * 这里只能使用异步刷写如磁盘 FlushRealTimeService
     */
    private final FlushCommitLogService commitLogService;

    /**
     * 追加进入字节数组实现实现【名字很奇怪】
     */
    private final AppendMessageCallback appendMessageCallback;


    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;


    /**
     * 记录消息数据在 topic-queueid中的索引坐标（每添加一个累加+1）
     */
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);


    private volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;


    private final PutMessageLock putMessageLock;

    /**
     * 构造CommitLog
     * @param defaultMessageStore
     */
    public CommitLog(final DefaultMessageStore defaultMessageStore) {

        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
            defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());

        this.defaultMessageStore = defaultMessageStore;

        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        this.commitLogService = new CommitRealTimeService();

        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
        batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
            @Override
            protected MessageExtBatchEncoder initialValue() {
                return new MessageExtBatchEncoder(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    /**
     * 加载
     * 对于已经写满的的文件同步更新对应ByteBuffer坐标位置到完结。
     * @return
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * 启动
     * 启动磁盘刷新服务
     */
    public void start() {
        this.flushCommitLogService.start();

        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    /**
     * 关闭
     * 关闭磁盘刷新服务
     */
    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }
        this.flushCommitLogService.shutdown();
    }

    /**
     * 刷盘
     * @return
     */
    public long flush() {
        //将writeBuffer数据写入fileChannel（getMessageStoreConfig().isTransientStorePoolEnable()开启才有用）
        this.mappedFileQueue.commit(0);
        //将通过flushedWhere获取MappedFileQueue中最后一个MappedFile调用flush将mappedByteBuffer或fileChannel中数据写入磁盘
        this.mappedFileQueue.flush(0);
        //获取刷盘的坐标
        return this.mappedFileQueue.getFlushedWhere();
    }



    /**
     * 获取最后一个MappedFile wrote位置，到queue中记录的commit的位置之差
     * @return
     */
    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    /**
     *  获取最后一个MappedFile commit位置，到queue中记录的flush的位置之差
     * @return
     */
    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    /************************  读取CommitLog文件队列star  ************************/
    /**
     * 查询offset偏移位置所在commitLog文件队列中mappedFile
     * 并获取mappedFile 从offset偏移位置开始的所有数据
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    /**
     * 查询offset偏移位置所在commitLog文件队列中mappedFile
     * 并获取mappedFile 从offset偏移位置开始的所有数据
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        // 通过offseth获取CommitLog下所属的mappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            // 读取文件MappedFile对应字节缓冲区pos偏移坐标开始的字节数据
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }
    /************************  读取CommitLog文件队列end  ************************/


    /************************  数据恢复star  ************************/


    /**
     * 正常退出时，数据恢复，所有内存数据都已刷新
     */
    public void recoverNormally() {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
    }


    /**
     * 异常恢复CommitLog内存数据
     */
    public void recoverAbnormally() {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                // Normal data
                if (size > 0) {
                    mappedFileOffset += size;

                    if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                        if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    } else {
                        this.defaultMessageStore.doDispatch(dispatchRequest);
                    }
                }
                // Intermediate file read error
                else if (size == -1) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
                // Come the end of the file, switch to the next file
                // Since the return 0 representatives met last hole, this can
                // not be included in truncate offset
                else if (size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // The current branch under normal circumstances should
                        // not happen
                        log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
        }
        // Commitlog case files are deleted
        else {
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }


    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        long storeTimestamp = byteBuffer.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
        if (0 == storeTimestamp) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    /************************  数据恢复end  ************************/

    private void notifyMessageArriving() {

    }


    /**
     * 追加消息
     * @param msg
     * @return
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // 设置消息的生成时间
        msg.setStoreTimestamp(System.currentTimeMillis());
        // 设置邮件正文BODY CRC
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // 返回结果
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        //获取消息的sysflag字段，检查消息是否是非事务性（第3/4字节为0）或者提交事务（commit，第4字节为1，第3字节为0）消息
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // 再从消息properties属性中获取"DELAY"参数（该参数在应用层通过Message.setDelayTimeLevel(int level)方法设置，消息延时投递时间级别，0表示不延时，大于0表示特定延时级别）属性的值（即延迟级别）
            // 若该值大于0，则将此消息设置为定时消息
            if (msg.getDelayTimeLevel() > 0) {

                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }
                //将MessageExtBrokerInner对象的topic值更改为SCHEDULE_TOPIC
                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                //根据延迟级别获取延时消息的队列ID（queueId等于延迟级别减去1）并更改queueId值
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // 将消息中原真实的topic和queueId存入消息属性中
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        //调用MapedFileQueue.getLastMapedFile方法获取或者创建最后一个文件（即MapedFile列表中的最后一个MapedFile对象），
        //若还没有文件或者已有的最后一个文件已经写满则创建一个新的文件，即创建一个新的MapedFile对象并返回；
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //加锁
        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // 设置消息的生成时间
            msg.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            //调用MapedFile.appendMessage(Object msg, AppendMessageCallback cb)方法将消息内容写入MapedFile.mappedByteBuffer：
            //MappedByteBuffer对象，即写入消息缓存中；由后台服务线程定时的将缓存中的消息刷盘到物理文件中；
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                /**
                 * 若最后一个MapedFile剩余空间不足够写入此次的消息内容，即返回状态为END_OF_FILE标记，
                 * 则再次调用MapedFileQueue.getLastMapedFile方法获取新的MapedFile对象然后调用MapedFile.appendMessage方法重写写入，
                 * 最后继续执行后续处理操作；若为PUT_OK标记则继续后续处理；若为其他标记则返回错误信息给上层
                  */
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // 再次调用MapedFileQueue.getLastMapedFile方法获取新的MapedFile对象
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    //调用MapedFile.appendMessage方法重写写入
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            //释放锁
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        //设置消息返回
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 统计
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        //根据刷盘策略刷盘
        handleDiskFlush(result, putMessageResult, msg);
        //主从同步
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }


    /**
     * 追加批量消息
     * @param messageExtBatch
     * @return
     */
    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        handleDiskFlush(result, putMessageResult, messageExtBatch);

        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     * 追加数据到startOffset所在MappedFile
     * @param startOffset
     * @param data
     * @return
     */
    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    /**
     * 刷写磁盘
     * @param result
     * @param putMessageResult
     * @param messageExt
     */
    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // 同步刷写
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            if (messageExt.isWaitStoreMsgOK()) {
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                service.putRequest(request);
                boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                        + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else {
                service.wakeup();
            }
        }
        // 异步刷写
        else {
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else {
                commitLogService.wakeup();
            }
        }
    }

    /**
     * 同步
     * @param result
     * @param putMessageResult
     * @param messageExt
     */
    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // Determine whether to wait
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    service.putRequest(request);
                    service.getWaitNotifyObject().wakeupAll();
                    boolean flushOK =
                        request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: "
                            + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

    }


    /**
     * 获取消息commitLog文件队列中的消息存储时间
     * @param offset  消息的物理偏移坐标
     * @param size    消息大小
     * @return
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        //校验offset
        if (offset >= this.getMinOffset()) {
            //返回消息结果
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    //获取消息的存储时间【MESSAGE_STORE_TIMESTAMP_POSTION表示存储时间的偏移】
                    return result.getByteBuffer().getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    /**
     * 获取消息commitLog文件队列中的消息数据
     * @param offset  消息的物理偏移坐标
     * @param size    消息大小
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }


    /**
     * 获取commitLog文件队列中存储最小的偏移坐标【==第一个mappedFile文件初始偏移坐标】
     * @return
     */
    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * 获取offset所属mappedFile,后一个mappedFile文件初始偏移坐标
     * @param offset
     * @return
     */
    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }


    /**
     * 获取commitLog文件队列中存储最最大的偏移坐标【==最后一个mappedFile文件初始偏移坐标+该文件已写入的坐标偏移】
     * @return
     */
    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }







    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    /**
     * 清除所有mappedFiles
     */
    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * 销毁第一个mappedFile
     * @param intervalForcibly
     * @return
     */
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }


    /**
     * 删除topic-queueid中的索引坐标
     * @param topic
     * @param queueId
     */
    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    /**
     * 检查mappedFiles中，除了最后一个文件，其余每一个mappedFile的大小是否是mappedFileSize
     */
    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    /**
     * @return 返回加锁时间
     */
    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                int commitDataThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread.
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 异步刷写服务类
     */
    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }


    /**
     * 同步刷写服务类请求
     */
    public static class GroupCommitRequest {
        /**
         * 本次追加消息后,下次追加坐标
         */
        private final long nextOffset;
        /**
         * 线程同步工具,用来同步阻塞线程是否刷写成功【如果消息配置了messageExt.isWaitStoreMsgOK()=true,
         * 追加消息的线程会调用waitForFlush同步阻塞等待，在超时时间内收到wakeupCustomer确认消息同步刷写成功才能返回】
         */
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        /**
         * 同步刷写成功标识
         */
        private volatile boolean flushOK = false;

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        public long getNextOffset() {
            return nextOffset;
        }


        /**
         * GroupCommitService 同步刷写服务类调用此方法通知同步刷写完毕,释放同步锁
         * @param flushOK
         */
        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }

        /**
         * 通过同步锁,等待同步刷写成功的同步
         * @param timeout
         * @return
         */
        public boolean waitForFlush(long timeout) {
            try {
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return this.flushOK;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
                return false;
            }
        }
    }

    /**
     * 同步刷写服务类，一个线程一直的处理同步刷写任务，每处理一个循环后等待10毫秒，一旦新任务到达，立即唤醒执行任务
     */
    class GroupCommitService extends FlushCommitLogService {
        // 保存同步刷写服务类请求集合
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        // 在同步刷写服务类中止关闭时，用来保存最后需要处理同步刷写服务类请求，数据会从requestsWrite拷贝过来？为什么这么做
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        /**
         * 添加同步刷写服务类请求
         * @param request
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            //设置父类hasNotified为true表示收到新的任务，释放线程中同步等待锁
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        /**
         * 置换requestsRead,requestsWrite
         */
        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 执行启动同步刷写服务类
         */
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            //进入work工作线程，只要工作线程状态stopped！=true 无限循环
            while (!this.isStopped()) {
                try {
                    //每执行一次，没有新的通知，同步等待10秒
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 关闭服务休眠10毫秒
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            //置换requestsRead,requestsWrite
            synchronized (this) {
                this.swapRequests();
            }

            //执行刷盘
            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                //优先刷盘requestsRead
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // 下一个文件中可能有一条消息，因此最多为刷新次数的两倍  ？为什么这么做
                        boolean flushOK = false;
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            //判断是否需要刷盘
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                            if (!flushOK) {
                                //从mappedFileQueue获取最后一个mappedFile将mappedFile内存映射中的数据写入磁盘
                                CommitLog.this.mappedFileQueue.flush(0);
                            }
                        }
                        //通知req线程同步释放锁。用来实现消息写入磁盘同步返回
                        req.wakeupCustomer(flushOK);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                } else {
                    //通知req线程同步释放锁。用来实现消息写入磁盘同步返回
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }



        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // 文件在最小固定长度结束时为空
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // 存储消息Id数据的Buffer
        private final ByteBuffer msgIdMemory;
        // 存储消息数据的Buffer（先写入msgStoreItemMemory，最后msgStoreItemMemory写入MappedFile对应的byteBuffer）
        private final ByteBuffer msgStoreItemMemory;
        // 存储消息brokersocker地址数据的Buffer
        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        // 单个消息存储在文件中的最大大小，默认为512K 从defaultMessageStore.getMessageStoreConfig().getMaxMessageSize()获取
        private final int maxMessageSize;

        private final StringBuilder keyBuilder = new StringBuilder();
        private final StringBuilder msgIdBuilder = new StringBuilder();

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        /**
         * 追加普通消息
         * @param fileFromOffset  文件名和对应偏移
         * @param byteBuffer  MappedFile对应字节缓冲区
         * @param maxBlank 校验长度
         * @param msgInner
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBrokerInner msgInner) {

            // 获取当前commitLog 物理偏移坐标
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 重置hostHolder,将消息brokersocker地址数据写入hostHolder中
            this.resetByteBuffer(hostHolder, 8);
            String msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(hostHolder), wroteOffset);

            // 从CommitLog.this.topicQueueTable获取消息在 topic-queue队列逻辑坐标，初始化为0，每添加一条+1
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // 获取消息类型
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // 当消息为事务消息中PREPARED，ROLLBACK设置 opic-queue队列逻辑坐标为0
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }


            //序列化消息propertiesString
            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
            //获取序列化消息propertiesString字节长度
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;
            //校验序列化消息propertiesString字节长度
            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }


            //序列化消息Topic
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            //获取序列化消息Topic字节长度
            final int topicLength = topicData.length;
            //获取消息体Body字节长度
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;



            //计算消息存储总字节长度
            final int msgLen = calMsgLength(bodyLength, topicLength, propertiesLength);
            // 校验总字节长度
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            //校验总字节长度，如果当前MappedFile文件没办法存储当前消息在MappedFile写入一条BLANK_MAGIC_CODE类型尾部消息
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                //设置msgStoreItemMemory区的limit为maxBlank
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 追加maxBlank长度
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 追加文件结束空MAGIC CODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 msgStoreItemMemory 添加到MappedFile对应 byteBuffer
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                // 4 返回AppendMessageResult 类型为END_OF_FILE 表示文件无法满足此消息写入，外部会创建一个新的MappedFile重新写入
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                    queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            // 重置msgStoreItemMemory
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 消息+元数据总长度
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 魔数，固定值 不知道干嘛
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 消息crc
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 队列id
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 flag
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 ConsumeQueue队列逻辑偏移坐标
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 CommitLog队列文件物理偏移坐标
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 sysflag
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 消息产生时间
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 消息产生的ip + port
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(hostHolder));
            // 11 消息存储时间
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 消息存储的ip + port
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(hostHolder));
            //this.msgBatchMemory.put(msgInner.getStoreHostBytes());
            // 13 重新消费的次数
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 事物相关坐标量
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 消息体长度
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 topic长度 topic
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 属性长度 属性
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData);

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // msgStoreItemMemory 添加到MappedFile对应 byteBuffer
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            //构造返回结果
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }
            return result;
        }

        /**
         * 追加批量消息
         * @param fileFromOffset
         * @param byteBuffer
         * @param maxBlank
         * @param messageExtBatch, backed up by a byte array
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBatch messageExtBatch) {

            //标记MappedFile对应字节缓冲区 pos坐标位置
            byteBuffer.mark();
            //获取当前commitLog 物理偏移坐标
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 从CommitLog.this.topicQueueTable获取消息在 topic-queue队列逻辑坐标，初始化为0，每添加一条+1
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;

            //记录批量消息总大小
            int totalMsgLen = 0;
            //记录批量消息的总数量
            int msgNum = 0;
            //重置记录消息ID字符串长度0
            msgIdBuilder.setLength(0);
            //写入开始时间
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();

            //获取MessageExtBatchEncoder.encode(messageExtBatch)的字节缓冲区对象
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();
            //标记messagesByteBuff pos位置
            messagesByteBuff.mark();

            //重置hostHolder,将消息brokersocker地址数据写入hostHolder中
            this.resetByteBuffer(hostHolder, 8);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(hostHolder);

            //遍历messagesByteBuff 一条消息一条消息的解析,覆盖messageExtBatch.getEncodedBuff()缓冲区中每条消息的
            //topic-queue队列逻辑坐标和CommitLog物理偏移坐标
            while (messagesByteBuff.hasRemaining()) {
                // 1 获取消息的pos
                final int msgPos = messagesByteBuff.position();
                // 2 读取消息+元数据总长度
                final int msgLen = messagesByteBuff.getInt();
                // 3 计算出消息的body大小长度
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // 校验批量消息中一条交易消息的长度
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                //校验总字节长度，如果当前MappedFile文件没办法存储当前消息在MappedFile写入一条BLANK_MAGIC_CODE类型尾部消息
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    //设置msgStoreItemMemory区的limit为maxBlank
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 追加maxBlank长度
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 追加文件结束空MAGIC CODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 重置messagesByteBuff
                    messagesByteBuff.reset();
                    // 4 重置byteBuffer
                    byteBuffer.reset();
                    // 5 msgStoreItemMemory 添加到MappedFile对应 byteBuffer
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    // 4 返回AppendMessageResult 类型为END_OF_FILE 表示文件无法满足此消息写入，外部会创建一个新的MappedFile重新写入
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                        beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //移动以添加队列偏移量到msgPos + 20  消息topic-queue队列逻辑坐标
                messagesByteBuff.position(msgPos + 20);

                /**
                 * 覆盖消息 ConsumeQueue队列逻辑偏移坐标 MessageExtBatchEncoder.encode方法中设置为0
                 * 6 QUEUEOFFSET
                 * this.msgBatchMemory.putLong(0);
                 */
                messagesByteBuff.putLong(queueOffset);
                /**
                 *  覆盖消息 CommitLog队列文件物理偏移坐标 MessageExtBatchEncoder.encode方法中设置为0
                 * 6 QUEUEOFFSET
                 * this.msgBatchMemory.putLong(0);
                 */
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                //设置position = 0
                storeHostBytes.rewind();
                //创创建消息ID写入msgIdBuilder
                String msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                //消息topic-queue队列逻辑坐标+1
                queueOffset++;
                //队列数据+1
                msgNum++;
                //移动以添加队列偏移量到此消息结算的位置，for循环读取下一条消息
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            //追加消息到MappeFile对字节缓冲区中
            byteBuffer.put(messagesByteBuff);
            //设置为null表示已经处理
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            //设置队列偏移
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    /**
     * 批量消息(MessageExtBatch)的编码器
     */
    public static class MessageExtBatchEncoder {
        // 存储批量消息内容的buffer
        private final ByteBuffer msgBatchMemory;
        // 最大阈值，批量消息长度不能超过该值
        private final int maxMessageSize;

        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        //size默认4M
        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        /**
         * 把messageExtBatch转换为ByteBuffer
         *
         * 设置到EncodedBuff属性中
         * messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));
         * 参考 AppendCallbackTest
         * @param messageExtBatch
         * @return
         */
        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            //清理
            msgBatchMemory.clear(); //not thread-safe
            int totalMsgLen = 0;
            /**
             * 当消息为MessageExtBatch 我们会把每个消息encodeMessage(message)字节数组累加并返回
             * List<Message> messages = new ArrayList<>();
             * messageExtBatch.setBody(MessageDecoder.encodeMessages(messages));
             *
             * 获取Body字节数组转化为字节缓冲区
             */
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();
            //依次遍历messagesByteBuff读取msg MessageExtBatch.body中多条消息
            while (messagesByteBuff.hasRemaining()) {
                // 1 读取消息+元数据总长度
                messagesByteBuff.getInt();
                // 2 读取MAGICCODE
                messagesByteBuff.getInt();
                // 3 读取BODYCRC
                messagesByteBuff.getInt();
                // 4 读取FLAG
                int flag = messagesByteBuff.getInt();
                // 5 读取BODY长度
                int bodyLen = messagesByteBuff.getInt();
                // 6 读取bodyCrc
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 7 读取properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);
                // 8 读取topicData
                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
                // 9 读取topicLength
                final int topicLength = topicData.length;
                // 10 读取msgLen
                final int msgLen = calMsgLength(bodyLen, topicLength, propertiesLen);

                // 校验批量消息中某一条消息长度
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }
                // 统计批量消息的总长度
                totalMsgLen += msgLen;
                // 校验批量消息总长度
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                //追加消息到字节缓冲区中
                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET  ConsumeQueue队列逻辑偏移坐标 这里暂时设置为0  DefaultAppendMessageCallback.doAppend覆盖
                this.msgBatchMemory.putLong(0);
                // 7 PHYSICALOFFSET CommitLog队列文件物理偏移坐标  这里暂时设置为0 DefaultAppendMessageCallback.doAppend覆盖
                this.msgBatchMemory.putLong(0);
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(hostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(hostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort(propertiesLen);
                if (propertiesLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        /**
         * 重置byteBuffer回到初始位置，设置limit，使得byteBuffer可以重现写入
         * @param byteBuffer
         * @param limit
         */
        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    /**
     * 检查消息并生成消息调度请求【用来同步创建消息对应index和consumequeue数据】
     * @param byteBuffer  selectMappedBuffer（ReputMessageService.reputFromOffset）获取得到MappeFile对应的MappedByteBuffer分片缓存区
     * @param checkCRC    是否检查消息CRC
     * @return
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }


    /**
     * 检查消息并生成消息调度请求【用来同步创建消息对应index和consumequeue数据】
     * @param byteBuffer  selectMappedBuffer（ReputMessageService.reputFromOffset）获取得到MappeFile对应的MappedByteBuffer分片缓存区
     * @param checkCRC    是否检查消息CRC
     * @param readBody    checkCRC=true 时此值必须为true,检查CRC需要获取body加密,此属性感觉很鸡肋
     * @return
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean readBody) {
        try {
            //获取消息+元数据总长度
            int totalSize = byteBuffer.getInt();

            //获取消息MAGICCODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                //消息标识
                case MESSAGE_MAGIC_CODE:
                    break;
                //文件尾部标识
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                //异常返回
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }
            //构造一个字节数组存储部分byteBuffer获取的数据
            byte[] bytesContent = new byte[totalSize];
            //获取消息加密消息crc
            int bodyCRC = byteBuffer.getInt();
            //获取消息存储队列id
            int queueId = byteBuffer.getInt();
            //获取消息flag
            int flag = byteBuffer.getInt();
            //获取消息ConsumeQueue队列逻辑偏移坐标
            long queueOffset = byteBuffer.getLong();
            //获取消息CommitLog队列文件物理偏移坐标（MappedByteBuffer.fileFromOffset + MappedByteBuffer.position()存储起始坐标）
            long physicOffset = byteBuffer.getLong();
            //获取消息sysflag同步
            int sysFlag = byteBuffer.getInt();
            //获取消息产生时间
            long bornTimeStamp = byteBuffer.getLong();
            //获取消息消息产生的ip + port写入到字节数组bytesContent 0 到 8 个字节
            ByteBuffer byteBuffer1 = byteBuffer.get(bytesContent, 0, 8);
            //获取消息存储时间
            long storeTimestamp = byteBuffer.getLong();
            //获取消息存储的ip + port写入到字节数组bytesContent 0 到 8 个字节(覆盖了上面的字节数组中的数据)
            ByteBuffer byteBuffer2 = byteBuffer.get(bytesContent, 0, 8);
            //获取消息重新消费的次数
            int reconsumeTimes = byteBuffer.getInt();
            //获取消息事物相关偏移量
            long preparedTransactionOffset = byteBuffer.getLong();
            //获取消息体的长度
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    //获取消息体数据写入字节数组中0 到 bodyLen 个字节(覆盖了上面的字节数组中的数据)
                    byteBuffer.get(bytesContent, 0, bodyLen);
                    //校验CRC
                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }
            //获取消息topic的长度
            byte topicLen = byteBuffer.get();
            //获取消息topic写入字节数组中0 到 topicLen 个字节(覆盖了上面的字节数组中的数据)
            byteBuffer.get(bytesContent, 0, topicLen);
            //获取消息topic
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            //获取 PROPERTIES LENGTH 数据数据的字节长度
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            //如果属性长度>0 获取数据线数据中信息
            if (propertiesLength > 0) {
                //获取消息属性写入字节数组中0 到 propertiesLength 个字节(覆盖了上面的字节数组中的数据)
                byteBuffer.get(bytesContent, 0, propertiesLength);

                //获取消息properties，转化为propertiesMap
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                //从propertiesMap获取keys,uniqKey,tags
                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // 定时消息处理
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (ScheduleMessageService.SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                    storeTimestamp);
                        }
                    }
                }
            }

            //校验消息长度和实现长度
            int readLength = calMsgLength(bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                        "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                        totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            return new DispatchRequest(
                    topic,
                    queueId,
                    physicOffset,
                    totalSize,
                    tagsCode,
                    storeTimestamp,
                    queueOffset,
                    keys,
                    uniqKey,
                    sysFlag,
                    preparedTransactionOffset,
                    propertiesMap
            );
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }


    /**
     * 删除过期文件,返回删除文件的数量
     * @param expiredTime      过期时间时间 如果文件最后修改时间+expiredTime < 当前时间则删除该文件
     * @param deleteFilesInterval    删除多个MappedFile文件的间隔时间
     * @param intervalForcibly       强制删除个MappedFile文件的间隔时间
     * @param cleanImmediately       是否需要强制删除
     * @return
     */
    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * 都清除掉offset偏移坐标之后的MappedFile,并重置offset偏移坐标MappedFile
     * @param offset
     * @return
     */
    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    /**
     * 计算消息存储总字节长度
     * @param bodyLength
     * @param topicLength
     * @param propertiesLength
     * @return
     */
    private static int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + 8 //BORNHOST
                + 8 //STORETIMESTAMP
                + 8 //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            if (log.isDebugEnabled()) {
                log.debug(String.valueOf(obj.hashCode()));
            }
        }
    }
}
