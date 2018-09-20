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
package org.apache.rocketmq.client.impl.producer;

import java.util.Set;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;

/**
 *
 */
public interface MQProducerInner {

    /**
     * 获取所有发送topic
     * @return
     */
    Set<String> getPublishTopicList();

    /**
     * 获取topic 发送路由信息是否更新
     * 1 我们在发送消息时会添加一个 new TopicPublishInfo()【没有messageQueue为】
     * 2 获取到路由信息时更新添加 messageQueue
     * @param topic
     * @return
     */
    boolean isPublishTopicNeedUpdate(final String topic);

    /**
     * 获取事务消息回调监听
     * @return
     */
    TransactionCheckListener checkListener();

    /**
     * 检查消息事务的状态
     * @param addr
     * @param msg
     * @param checkRequestHeader
     */
    void checkTransactionState(
        final String addr,
        final MessageExt msg,
        final CheckTransactionStateRequestHeader checkRequestHeader);

    /**
     * 更新topic对应的路由信息
     * MQClientInstance.updateTopicRouteInfoFromNameServer 定时从nameSrv获取路由信息时同步
     * @param topic
     * @param info
     */
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    /**
     * 是否是单元
     * @return
     */
    boolean isUnitMode();
}
