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

/**
 * $Id: NamesrvConfig.java 1839 2013-05-16 02:12:02Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.namesrv;

import java.io.File;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamesrvConfig {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    /**
     * RocketMQ安装目录
     * 如果没有指定的话，默认值为系统环境变量ROCKETMQ_HOME
     * 通过System.getenv获取，可以在~/.profile中export
     * 或者可以在配置文件中指定rocketmqHome=***
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    /**
     * KV配置持久化地址
     * 默认为System.getProperty("user.home")/namesrv/kvConfig.json文件
     */
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";
    /**
     * 持久化配置路径
     * 默认为System.getProperty("user.home")/namesrv/namesrv.properties文件
     */
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";

    private String productEnvName = "center";


    private boolean clusterTest = false;

    /**
     * 是否开启orderMessage 表示是保障消息的严格的一致性
     *
     * producer向broker发送消息前会通过namesrv获取topic路由信息【说白了就是每个brokerName中 master提供了多少个MessageQueue用来写入message】，在
     *
     * 所有的broker中选择一个MessageQueue 如 brokerName1:1;brokerName2:1;brokerName3:1; 这时候如果发送消息
     *
     * 消息ID：100 -->   brokerName1[中的一个MessageQueue[0]]
     *
     * 消息ID：101 -->   brokerName2[中的一个MessageQueue[0]]
     *
     * 消息ID：102 -->   brokerName3[中的一个MessageQueue[0]]
     *
     * 如果此时brokerName3 下线
     *
     * 消息ID：102 按照路由规则转移到其他机器  brokerName1[中的一个MessageQueue[0]] 导致消息不一致
     *
     * 如果开启orderMessage 在发送消息时会读取读取的路由信息优先从namesrv.KvConfigManager获取这里配置不会伴随brokerName改变保证消息顺序
     *
     * String orderTopicConf =
     *                     this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
     *                         requestHeader.getTopic())
     *
     * @return
     */
    private boolean orderMessageEnable = false;


    public boolean isOrderMessageEnable() {
        return orderMessageEnable;
    }

    public void setOrderMessageEnable(boolean orderMessageEnable) {
        this.orderMessageEnable = orderMessageEnable;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }

    public String getProductEnvName() {
        return productEnvName;
    }

    public void setProductEnvName(String productEnvName) {
        this.productEnvName = productEnvName;
    }

    public boolean isClusterTest() {
        return clusterTest;
    }

    public void setClusterTest(boolean clusterTest) {
        this.clusterTest = clusterTest;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(final String configStorePath) {
        this.configStorePath = configStorePath;
    }
}
