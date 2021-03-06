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
import java.io.RandomAccessFile;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.store.config.BrokerRole.SLAVE;

public class DefaultMessageStore implements MessageStore {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 消息存储配置
     */
    private final MessageStoreConfig messageStoreConfig;

    /**
     * commitLog:MQ将消息数据核心处理类
     */
    private final CommitLog commitLog;

    /**
     * topic-queueId 对应ConsumeQueue   ConsumeQueue:MQ将消息坐标数据核心处理类
     */
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    /**
     * ConsumeQueue刷盘服务
     * 扫描ConsumeQueue队列文件，将字节缓冲区的数据写入磁盘
     */
    private final FlushConsumeQueueService flushConsumeQueueService;

    /**
     * 定时清除CommitLog服务线程
     */
    private final CleanCommitLogService cleanCommitLogService;

    /**
     * 定时清除ConsumeQueue服务线程
     */
    private final CleanConsumeQueueService cleanConsumeQueueService;

    /**
     * 索引服务
     */
    private final IndexService indexService;

    /**
     * MappedFile分配线程，RocketMQ使用内存映射处理commitlog,consumeQueue文件
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 消息调度请求服务
     * 读取CommitLog，将消息的位置数据,同步到ConsumeQueue
     * 读取CommitLog，将消息的索引数据,同步到Index
     */
    private final ReputMessageService reputMessageService;

    /**
     * 主从同步实现服务
     */
    private final HAService haService;

    /**
     * 定时任务调度器，执行定时任务，主要是处理定时任务。
     */
    private final ScheduleMessageService scheduleMessageService;

    /**
     * 存储统计服务
     */
    private final StoreStatsService storeStatsService;

    /**
     * DataBuffer池
     */
    private final TransientStorePool transientStorePool;

    /**
     * 存储服务状态
     */
    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));


    /**
     * Broker统计服务
     */
    private final BrokerStatsManager brokerStatsManager;


    /**
     * 消息达到监听器
     */
    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        this.commitLog = new CommitLog(this);
        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        this.haService = new HAService(this);

        this.reputMessageService = new ReputMessageService();

        this.scheduleMessageService = new ScheduleMessageService(this);

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.allocateMappedFileService.start();

        this.indexService.start();

        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }


    /************************  DefaultMessageStore其他方法 star  ************************/

    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }
    /************************  DefaultMessageStore其他方法 end  ************************/


    /**
     * DefaultMessageStore 加载
     *
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;

        try {
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }

            result = result && this.commitLog.load();

            result = result && this.loadConsumeQueue();

            if (result) {
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                this.indexService.load(lastExitOK);

                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    private void recover(final boolean lastExitOK) {
        this.recoverConsumeQueue();

        if (lastExitOK) {
            this.commitLog.recoverNormally();
        } else {
            this.commitLog.recoverAbnormally();
        }

        this.recoverTopicQueueTable();
    }

    private void recoverConsumeQueue() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
            }
        }
    }

    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQueue());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }

    /**
     * @throws Exception
     */
    public void start() throws Exception {

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);

        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        if (this.scheduleMessageService != null && SLAVE != messageStoreConfig.getBrokerRole()) {
            this.scheduleMessageService.start();
        }

        if (this.getMessageStoreConfig().isDuplicationEnable()) {
            this.reputMessageService.setReputFromOffset(this.commitLog.getConfirmOffset());
        } else {
            this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
        }
        this.reputMessageService.start();

        this.haService.start();

        this.createTempFile();
        this.addScheduleTask();
        this.shutdown = false;
    }

    /**
     * 创建abort跟目录文件
     *
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }


    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {

                Thread.sleep(1000 * 3);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }

            this.haService.shutdown();

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        //检测操作系统页写入是否忙
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        //将日志写入CommitLog文件，具体实现类CommitLog
        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessage(msg);
        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBody().length);
        }

        //记录相关统计信息
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        //记录写commitlog失败次数
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        if (this.shutdown) {
            log.warn("DefaultMessageStore has shutdown, so putMessages is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("DefaultMessageStore is in slave mode, so putMessages is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("DefaultMessageStore is not writable, so putMessages is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("PutMessages topic length too long " + messageExtBatch.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessages(messageExtBatch);

        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, messageExtBatch.getBody().length);
        }
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    /**
     * 检测操作系统页写入是否忙
     *
     * @return
     */
    @Override
    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        return diff < 10000000
                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }


    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }


    /*********************** ha相关函数star  ***********************/
    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    /*********************** ha相关函数end  ***********************/

    @Override
    public long now() {
        return this.systemClock.now();
    }


    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    /*********************** commitLog相关函数star  ***********************/

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 通过消息物理偏移坐标和长度
     * 查询消在commitLog文件队列中存储数据
     *
     * @param commitLogOffset commit log offset.
     * @param msgSize         message size.
     * @return
     */
    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    /**
     * 获取最新物理偏移坐标对应消息的创建时间
     *
     * @return
     */
    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    /**
     * 查询offset偏移位置所在commitLog文件队列中mappedFile
     * 并获取mappedFile 从offset偏移位置开始的所有数据
     *
     * @param offset starting offset.
     * @return
     */
    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    /**
     * 追加数据到startOffset所在MappedFile
     *
     * @param startOffset starting offset.
     * @param data        data to append.
     * @return
     */
    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    /**
     * 清理掉topic集合所有topic在commitLog中的索引坐标
     *
     * @param topics all valid topics.
     * @return
     */
    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                            cq.getTopic(),
                            cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    @Override
    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                                nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId(),
                                nextQT.getValue().getMaxPhysicOffset(),
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                                topic,
                                nextQT.getKey(),
                                minCommitLogOffset,
                                maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    /**
     * 重置commitLog文件队列最大偏移位置
     */
    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    /**
     * 获取commitLog  confirmOffset
     */
    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    /**
     * 设置commitLog  confirmOffset
     */
    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }
    /*********************** commitLog相关函数end  ***********************/

    /*********************** getMessage相关函数star  ***********************/

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
                                       final int maxMsgNums,
                                       final MessageFilter messageFilter) {
        //校验是否关闭
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        //交易是否可读
        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        //获取系统当前时间
        long beginTime = this.getSystemClock().now();
        //默认返回查询状态
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;

        //下次查询consumeQueue索引坐标
        long nextBeginOffset = offset;
        //consumeQueue 最小偏移的索引坐标
        long minOffset = 0;
        //ConsumeQueue写入消息最大索引坐标
        long maxOffset = 0;

        //构造返回结果对象
        GetMessageResult getResult = new GetMessageResult();

        //获取commitLog文件队列中存储最最大的偏移坐标
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        //查找topic-queueId 对应ConsumeQueue
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            //获取consumeQueue 最小偏移的索引坐标
            minOffset = consumeQueue.getMinOffsetInQueue();
            //获取ConsumeQueue写入消息最大索引坐标
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {
                //获取索引坐标为startIndex的消息所在mappedFile的字节缓冲区分片【pos=startIndex * CQ_STORE_UNIT_SIZE% mappedFileSize】
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        //设置消息返回结果
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        //一次查询获取的消息数量不能超时16000/ConsumeQueue.CQ_STORE_UNIT_SIZE
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        //遍历获取消息
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 获取消息 CommitLog 偏移坐标
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            // 获取消息 消息长度
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            // 获取消息 tagsCode
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            maxPhyOffsetPulling = offsetPy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }
                            //检查校验offsetPy偏移坐标开始读取之后所有数据开始数据是否能从磁盘独处全部写入内存
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                    isInDisk)) {
                                break;
                            }

                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                            tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            if (messageFilter != null
                                    && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            if (messageFilter != null
                                    && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                            getResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        if (diskFallRecorded) {
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }

                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                                * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    //查询消息物理偏移位置没有在ConsumeQueue文件队列中找到
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            //查询消息topic-queueId 对应ConsumeQueue不存在
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            // 统计查询到消息的次数
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            //统计没有查询到消息的次数
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        //记录并统计计算最大查询时间
        long eclipseTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(eclipseTime);

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    /**
     * 根据消息索引key和消息产生时间查询消息
     * @param topic  topic of the message.  消息topic
     * @param key    message key.             消息key
     * @param maxNum maximum number of the messages possible. 返回满足消息的最大数量
     * @param begin  begin timestamp.       查询消息产生时间开始
     * @param end    end timestamp.           查询消息产生时间结束
     * @return
     */
    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            //通过索引服务查询消息的物理偏移坐标
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }
            //排序
            Collections.sort(queryOffsetResult.getPhyOffsets());
            //最后一次更新索引消息物理偏移
            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            //最后一次更新索引数据时间
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    //通过消息物理偏移坐标查询消息
                    MessageExt msg = this.lookMessageByOffset(offset);

                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    /**
     * 通过消息物理偏移从commitLog文件队列中查询消息数据
     * @param commitLogOffset physical offset.
     * @return
     */
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 获取消息的大小
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 通过消息物理偏移和大小从commitLog文件队列中查询消息数据
     * @param commitLogOffset
     * @param size
     * @return
     */
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 获取topic-queueId对应consumeQueue文件队列中索引位置从minOffset到maxOffset所有消息物理偏移坐标和消息ID
     * @param topic
     * @param queueId
     * @param minOffset
     * @param maxOffset
     * @param storeHost
     * @return
     */
    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
                                           SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }
        //查找创建topic queueId 对应ConsumeQueue
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            //校验minOffset maxOffset
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }
            //遍历条件
            long nextOffset = minOffset;
            //从minOffset开始一条提条读取
            while (nextOffset < maxOffset) {
                //获取索引坐标为startIndex的消息所在mappedFile的字节缓冲区分片
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        //遍历字节缓冲区中的每天消息的对应的坐标信息
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            //获取坐标信息中物理偏移位置
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                            //获取消息ID
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            //设置到messageIds
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }


    /**
     * 订正查询consumeQueue索引坐标
     * @param oldOffset
     * @param newOffset
     * @return
     */
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    /**
     * @param sizePy   当前消息大小
     * @param maxMsgNums   总共需要获取消息的数量
     * @param bufferTotal
     * @param messageTotal
     * @param isInDisk
     * @return
     */
    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (maxMsgNums <= messageTotal) {
            return true;
        }

        if (isInDisk) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    /*********************** getMessage相关函数end  ***********************/

    /*********************** 磁盘文件相关函数star  ***********************/
    /**
     * 删除文件
     *
     * @param fileName
     */
    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * 判断abort目录是否存在
     *
     * @return
     */
    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }


    /**
     * 检查校验某个物理偏移坐标开始读取之后所有数据开始数据是否能从磁盘独处全部写入内存
     *
     * @param offsetPy
     * @param maxOffsetPy
     * @return
     */
    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    /**
     * 获取topic-queueId 坐标所索引对应的第一条消息数据物理偏移坐标
     * 检查校验某个物理偏移开始数据是否能从磁盘独处全部写入内存（监测机器内存是否够填充磁盘数据）
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.  consume
     * @return
     */
    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {
        //获取commitLog文件队列中存储最最大的偏移坐标
        final long maxOffsetPy = this.commitLog.getMaxOffset();
        //从consumeQueueTable查找topic queueId 对应ConsumeQueue
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            //获取索引坐标为startIndex的消息所在mappedFile的字节缓冲区分片ByteBuffer其中pos=startIndex * CQ_STORE_UNIT_SIZE% mappedFileSize
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    //读取物理偏移坐标检测
                    //检查校验某个物理偏移坐标开始读取之后所有数据开始数据是否能从磁盘独处全部写入内存
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }
    /*********************** 磁盘文件相关函数end  ***********************/


    /*********************** transientStorePool相关函数star  ***********************/
    /**
     * transientStorePool剩余可借用的buffers数量
     *
     * @return
     */
    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.remainBufferNumbs();
    }

    /**
     * transientStorePool没有可以借用的ByteBuffer
     *
     * @return
     */
    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }
    /*********************** transientStorePool相关函数end   ***********************/


    /*********************** ConsumeQueue相关函数star  ***********************/

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }
        return 0;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    /**
     * 查询topic-queueId ConsumeQueue最小物理偏移坐标消息的创建事件
     *
     * @param topic   Topic of the messages to query.
     * @param queueId Queue ID to find.
     * @return
     */
    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            //获取ConsumeQueue文件队列中最小CommitLog物理偏移坐标
            long minLogicOffset = logicQueue.getMinLogicOffset();
            //获取索引坐标为startIndex的消息所在mappedFile的字节缓冲区分片
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            //获取ConsumeQueue文件队列mappedFile对应字节缓冲区中第一条消息存储时间
            return getStoreTime(result);
        }

        return -1;
    }

    /**
     * 查询topic-queueId 指定物理偏移坐标消息的创建事件
     *
     * @param topic              message topic.
     * @param queueId            queue ID.
     * @param consumeQueueOffset consume queue offset.
     * @return
     */
    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    /**
     * 获取ConsumeQueue文件队列mappedFile对应字节缓冲区中第一条消息存储时间
     *
     * @param result
     * @return
     */
    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }


    /**
     * 获取ConsumeQueue队列文件写入消息的数量
     *
     * @param topic   Topic
     * @param queueId Queue ID.
     * @return
     */
    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    /**
     * 获取ConsumeQueue
     *
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return
     */
    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }


    /**
     * 查找或创建topic queueId 对应ConsumeQueue
     *
     * @param topic
     * @param queueId
     * @return
     */
    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
                    this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    /**
     * 加载硬盘中ConsumeQueue文件队列数据到添加到consumeQueueTable
     *
     * @return
     */
    private boolean loadConsumeQueue() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        ConsumeQueue logic = new ConsumeQueue(
                                topic,
                                queueId,
                                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
                                this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    /**
     * 添加ConsumeQueue
     *
     * @param topic
     * @param queueId
     * @param consumeQueue
     */
    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    /**
     * 获取ConsumeQueue最大偏移索引
     *
     * @param topic   Topic name.
     * @param queueId Queue ID.
     * @return
     */
    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    /**
     * 获取consumeQueue 最小偏移索引
     *
     * @param topic   Topic name.
     * @param queueId Queue ID.
     * @return
     */
    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }
    /*********************** ConsumeQueue相关函数end  ***********************/

    /**
     * unlockMappedFile
     *
     * @param mappedFile
     */
    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    /*********************** 定时任务star  ***********************/
    /**
     * 添加定时任务
     */
    private void addScheduleTask() {

        //定时清理过期文件
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        //监测所有ConsumeQueue，consumeQueueExt文件队列中文件大小
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        //开启打印线程堆栈
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                        + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }

    /**
     * 监测所有ConsumeQueue，consumeQueueExt文件队列中文件大小
     */
    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    /**
     * 清理过期文件
     */
    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }

    /**
     * 定时清理过期的CommitLog队列文件服务
     */
    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;

        /**
         * 检查磁盘是否达到了需要清理的阀值
         */
        private final double diskSpaceWarningLevelRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        /**
         * 检查磁盘是否达到了需要清理的阀值
         */
        private final double diskSpaceCleanForciblyRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));


        private long lastRedeleteTimestamp = 0;

        private volatile int manualDeleteFileSeveralTimes = 0;

        private volatile boolean cleanImmediately = false;

        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        public void run() {
            try {
                this.deleteExpiredFiles();

                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * 删除过期的commitlog文件【根据文件过期时间是否超过阀值】
         */
        private void deleteExpiredFiles() {
            //清理文件数量
            int deleteCount = 0;
            //在删除日志文件之前保留日志文件的小时数（以小时为单位）
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            //删除多个CommitLog文件的间隔时间（单位毫秒）
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            //强制删除文件间隔时间（单位毫秒）
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            //是否达到定时清理的时间
            boolean timeup = this.isTimeToDelete();
            //检查磁盘是否达到了需要清理的阀值
            boolean spacefull = this.isSpaceToDelete();
            //是手动触发删除
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            //是否满足清理条件
            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                //是否开启立刻强制清理文件
                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                        fileReservedTime,
                        timeup,
                        spacefull,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce);

                //换算小时对应的超时时间转化为毫秒
                fileReservedTime *= 60 * 60 * 1000;
                //删除过期文件,返回删除文件的数量
                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                        destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        /**
         * 删除commitlog队列第一个文件【依据执行时间间隔大于阀值】
         */
        private void redeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                //强制删除文件间隔时间（单位毫秒）
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        /**
         * 清理条件之一：是否达到定时清理的时间
         *
         * @return
         */
        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        /**
         * 清理条件之一：检查磁盘是否达到了需要清理的阀值
         *
         * @return
         */
        private boolean isSpaceToDelete() {
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            //标记是否需要清理
            cleanImmediately = false;

            /**
             * CommitLog 文件是否达到预警清理指标
             */
            {
                //获取commitLog存储路径
                String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                //获取commitLog存储路径磁盘已用空间占比 usedSpace / (double) totalSpace
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                //当已用空间占比大于diskSpaceWarningLevelRatio 默认（90%） 设置flagBits 状态加上DISK_FULL_BIT
                if (physicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }
                    //更新cleanImmediately
                    cleanImmediately = true;
                    //当已用空间占比大于diskSpaceCleanForciblyRatio 默认85%
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    //更新cleanImmediately
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }
                //当已用已用完直接返回立刻开始清理
                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            /**
             * ConsumeQueue 文件是否达到预警清理指标
             */
            {
                //获取ConsumeQueue存储路径
                String storePathLogics = StorePathConfigHelper
                        .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
                //获取ConsumeQueue存储路径磁盘已用空间占比 usedSpace / (double) totalSpace
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                //当已用空间占比大于diskSpaceWarningLevelRatio 默认（90%） 设置flagBits 状态加上DISK_FULL_BIT
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }
                    //更新cleanImmediately
                    cleanImmediately = true;
                    //当已用空间占比大于diskSpaceCleanForciblyRatio 默认85%
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    //更新cleanImmediately
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }
                //当已用已用完直接返回立刻开始清理
                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    /**
     * 定时清理过期的ConsumeQueue队列文件服务
     */
    class CleanConsumeQueueService {
        //记录当前清理偏移坐标
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            //获取定时清理过期的ConsumeQueue服务每次执行间隔
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();
            //获取commitLog文件队列中存储偏移坐标最小的mappedFile
            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            //对比偏移坐标判断是否需要清理
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                        //删除从ConsumeQueue队列文件中,删除偏移坐标小于minOffset数据
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    /**
     * 扫描ConsumeQueue队列文件，将字节缓冲区的数据刷写入磁盘
     * <p>
     * 【CommitLog刷写如磁盘服务在CommitLog内部类中，没有和ConsumeQueue保持一致】
     */
    class FlushConsumeQueueService extends ServiceThread {
        /**
         * 每次刷盘服务执行刷盘的次数
         */
        private static final int RETRY_TIMES_OVER = 3;

        /**
         * 记录每次刷盘服务执行时间
         */
        private long lastFlushTimestamp = 0;


        /**
         * 表示一次执行刷盘
         *
         * @param retryTimes 刷盘次数
         */
        private void doFlush(int retryTimes) {
            //获取刷新ConsumeQueue数据到磁盘条件页数
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            //如果retryTimes==RETRY_TIMES_OVER
            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            //获取刷新ConsumeQueue数据到磁盘服务每次执行间隔，判断是否执行本次刷盘
            long logicsMsgTimestamp = 0;
            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                //更新logicsMsgTimestamp
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            //遍历所有ConsumeQueue执行刷盘
            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;
            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                //StoreCheckpoint 刷盘
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }
    /*********************** 定时任务end  ***********************/

    /*********************** 调度请求同步服务star  ***********************/
    /**
     * 消息调度请求服务
     * 读取CommitLog，将消息的位置数据,同步到ConsumeQueue
     * 读取CommitLog，将消息的索引数据,同步到Index
     */
    class ReputMessageService extends ServiceThread {

        /**
         * 记录CommitLog 已经同步消息的偏移坐标  当 reputFromOffset< MappedFile.getReadPosition(字节缓冲区的写入位置)，则会触发同步。
         */
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        /**
         * 关闭服务
         */
        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        /**
         * 获取消息没有同步偏移量
         *
         * @return
         */
        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        /**
         * 是否存在需要同步的消息数据
         *
         * @return
         */
        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        private void doReput() {
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                // TODO 疑问：这个是啥
                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                        && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 获取CommitLog文件队列reputFromOffset偏移坐标所属的mappedFile
                // 获取从mappedFile.ByteBuffer(reputFromOffset --> MappedFile.getReadPosition)分片【用来分片】
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        //更新reputFromOffset=ByteBuffer.pos
                        this.reputFromOffset = result.getStartOffset();
                        // 遍历result中数据中每一条Message消息构造成一条调度请求同步生成
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                            // 读取ByteBuffer字节缓存区的一条Message构造成一条调度请求同步生成
                            DispatchRequest dispatchRequest =
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            int size = dispatchRequest.getMsgSize();
                            if (dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    //交给CommitLogDispatcher 处理请求，这里有2个分别对应ConsumeQueue和IndexFile
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);
                                    // 通知有新消息
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                    }
                                    //更新reputFromOffset
                                    this.reputFromOffset += size;
                                    //累加readSize
                                    readSize += size;
                                    // 统计
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).incrementAndGet();
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                                .addAndGet(dispatchRequest.getMsgSize());
                                    }
                                    // 读取文件尾部标识
                                } else if (size == 0) {
                                    //获取reputFromOffset 坐标对应mappedFile的下一个mappedFile其实物理坐标更新reputFromOffset
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    //累加readSize
                                    readSize = result.getSize();
                                }
                                // 读取失败
                            } else if (!dispatchRequest.isSuccess()) {
                                // 读取到Message构造成一条调度请求 发现异常BUG
                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                }
                                // 读取到Message是Blank却不是Blank  发现异常BUG
                                else {
                                    doNext = false;
                                    if (DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]the master dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                this.reputFromOffset);

                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }

    /**
     * 获取消息还需要同步ConsumeQueue Index偏移量
     *
     * @return
     */
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }


    public void doDispatch(DispatchRequest req) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    /**
     * CommitLog消息数据同步到 index文件队列中
     * indexService 是索引的核心实现
     */
    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    /**
     * CommitLog消息数据同步到ConsumeQueue文件队列中
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }


    /**
     * 将CommitLog存放的消息数据位置信息同步追加到ConsumeQueue
     *
     * @param dispatchRequest
     */
    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        cq.putMessagePositionInfoWrapper(dispatchRequest);
    }
    /*********************** 调度请求同步服务end  ***********************/

    /**
     * 返回处理调度请求的实现类
     * this.dispatcherList = new LinkedList<>();
     * this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());//同步ConsumeQueue
     * this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());//同步Index
     *
     * @return
     */
    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }
}
