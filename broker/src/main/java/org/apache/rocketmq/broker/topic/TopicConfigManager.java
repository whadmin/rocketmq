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
package org.apache.rocketmq.broker.topic;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * broker内部存储的topic管理器.
 */
public class TopicConfigManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 锁时间
     */
    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    /**
     * 读写锁
     */
    private transient final Lock lockTopicConfigTable = new ReentrantLock();

    /**
     * key是topicName，value是主题对应的配置TopicConfig
     */
    private final ConcurrentMap<String, TopicConfig> topicConfigTable =
            new ConcurrentHashMap<String, TopicConfig>(1024);

    /**
     * 配置的版本
     */
    private final DataVersion dataVersion = new DataVersion();

    /**
     * 系统配置的topic
     */
    private final Set<String> systemTopicList = new HashSet<String>();


    /**
     *
     */
    private transient BrokerController brokerController;

    public TopicConfigManager() {
    }

    /**
     * 构造TopicConfigManager
     *
     * @param brokerController 设置系统配置默认的topic
     *                         MixAll.SELF_TEST_TOPIC
     *                         MixAll.DEFAULT_TOPIC【brokerController.getBrokerConfig().isAutoCreateTopicEnable()=tue】
     *                         MixAll.BENCHMARK_TOPIC
     *                         MixAll.OFFSET_MOVED_EVENT
     *                         topic名称为BrokerName
     *                         topic名称为ClusterName
     */
    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        {
            // MixAll.SELF_TEST_TOPIC
            String topic = MixAll.SELF_TEST_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // MixAll.DEFAULT_TOPIC
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                String topic = MixAll.DEFAULT_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                this.systemTopicList.add(topic);
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig()
                        .getDefaultTopicQueueNums());
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig()
                        .getDefaultTopicQueueNums());
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // MixAll.BENCHMARK_TOPIC
            String topic = MixAll.BENCHMARK_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1024);
            topicConfig.setWriteQueueNums(1024);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName();
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            String topic = this.brokerController.getBrokerConfig().getBrokerName();
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isBrokerTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // MixAll.OFFSET_MOVED_EVENT
            String topic = MixAll.OFFSET_MOVED_EVENT;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
    }

    /**
     * 是否是系统配置的topic
     *
     * @param topic
     * @return
     */
    public boolean isSystemTopic(final String topic) {
        return this.systemTopicList.contains(topic);
    }

    /**
     * 获取系统配置的topic
     *
     * @return
     */
    public Set<String> getSystemTopic() {
        return this.systemTopicList;
    }

    /**
     * 发送消息topic 不能是 MixAll.DEFAULT_TOPIC
     * MixAll.DEFAULT_TOPIC 相当于系统预留的
     */
    public boolean isTopicCanSendMessage(final String topic) {
        return !topic.equals(MixAll.DEFAULT_TOPIC);
    }

    /**
     * 获取topic对应的配置
     *
     * @param topic
     * @return
     */
    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }


    /**
     * 场景
     * 1 Producer 发送消息前会通过 Producer 或 admin 发送请求到 broker创建topic和对应的TopicConfig
     * 2 Producer 发送消息前没有向broker申请topic和对应的配置直接发送消息,在发送消息的方法中，创建topic,返回对应的TopicConfig 【isAutoCreateTopicEnable()=false是不会出现此类场景】
     * 2-0  broker 启动时isAutoCreateTopicEnable()=true 会向namesrv注册MixAll.DEFAULT_TOPIC和对应的配置
     * 2-1  Producer 会向namesrv获取topic对应的TopicRouteData路由信息,此时会获取不到
     * 2-2  Producer 会向namesrv获取MixAll.DEFAULT_TOPIC对应TopicConfig
     * 2-3  Producer 会向broker发送消息同时会创建topic
     * <p>
     * 创建更新topic，如果是新建一个topic  TopicConfig参考defaultTopic，和clientDefaultTopicQueueNums
     *
     * @param topic
     * @param defaultTopic
     * @param remoteAddress
     * @param clientDefaultTopicQueueNums
     * @param topicSysFlag
     * @return
     */
    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
                                                      final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //从topicConfigTable 获取topic对应TopicConfig存在直接返回
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    //从topicConfigTable 获取defaultTopic对应TopicConfig
                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);
                    if (defaultTopicConfig != null) {
                        //如果defaultTopic是MixAll.DEFAULT_TOPIC 设置可以拥有读写权限
                        if (defaultTopic.equals(MixAll.DEFAULT_TOPIC)) {
                            if (!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }
                        //如果default主题的权限是可以继承的
                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            topicConfig = new TopicConfig(topic);

                            int queueNums =
                                    clientDefaultTopicQueueNums > defaultTopicConfig.getWriteQueueNums() ? defaultTopicConfig
                                            .getWriteQueueNums() : clientDefaultTopicQueueNums;

                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();
                            //当前topic 擦除"继承"的权限
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            log.warn("Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                                    defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                                defaultTopic, remoteAddress);
                    }

                    //方法进行到此说明是新创建的topic,TopicConfig来源一个系统配置的topic
                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]",
                                defaultTopic, topicConfig, remoteAddress);
                        //添加到topicConfigTable
                        this.topicConfigTable.put(topic, topicConfig);

                        //有新的topic版本更新
                        this.dataVersion.nextVersion();

                        //设置新创建标记
                        createNew = true;

                        //持久化到配置文件
                        this.persist();
                    }
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true);
        }

        return topicConfig;
    }

    /**
     * 创建更新topic
     *
     * @param topic
     * @param clientDefaultTopicQueueNums
     * @param perm
     * @param topicSysFlag
     * @return
     */
    public TopicConfig createTopicInSendMessageBackMethod(
            final String topic,
            final int clientDefaultTopicQueueNums,
            final int perm,
            final int topicSysFlag) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(topic, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true);
        }

        return topicConfig;
    }

    /**
     * 更新TopicConfig  FLAG_UNIT
     *
     * @param topic
     * @param unit
     */
    public void updateTopicUnitFlag(final String topic, final boolean unit) {

        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                    topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true);
        }
    }

    /**
     * 更新TopicConfig  FLAG_UNIT_SUB
     *
     * @param topic
     */
    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                    topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true);
        }
    }

    /**
     * 更新TopicConfig
     */
    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    /**
     * 更新topicConfigTable，以orderKVTableFromNs为准
     * topicConfigTable记录无order的 但是orderKVTableFromNs有记录的，改为有order的
     * topicConfigTable记录有order的 但是orderKVTableFromNs无记录的，改为无order的
     *
     * @param orderKVTableFromNs 从nameServer获取的 orderKVTable，里面全代表是有order顺序的
     */
    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            for (Map.Entry<String, TopicConfig> entry : this.topicConfigTable.entrySet()) {
                String topic = entry.getKey();
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = entry.getValue();
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }

            if (isChange) {
                this.dataVersion.nextVersion();
                this.persist();
            }
        }
    }

    /**
     * TopicConfig 是否是严格的顺序
     *
     * @param topic
     * @return
     */
    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig == null) {
            return false;
        } else {
            return topicConfig.isOrder();
        }
    }

    /**
     * 删除topic
     *
     * @param topic
     */
    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic config failed, topic: {} not exists", topic);
        }
    }


    /**
     * 获取TopicConfigManager 可序列化的对象
     *
     * @return
     */
    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper;
    }

    /**
     * 获取TopicConfigManager持久化配置文件
     *
     * @return
     */
    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    /**
     * 解码
     *
     * @param jsonString
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                    TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }

    /**
     * 编码
     *
     * @return
     */
    @Override
    public String encode() {
        return encode(false);
    }

    /**
     * 编码
     *
     * @param prettyFormat
     * @return
     */
    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 打印
     *
     * @param tcs
     */
    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        Iterator<Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }
}
