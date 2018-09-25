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

package org.apache.rocketmq.common.subscription;

import org.apache.rocketmq.common.MixAll;

/**
 * 用来描述Consumer分组在broker服务端应配置信息
 */
public class SubscriptionGroupConfig {

    /**
     * Consumer分组名称
     */
    private String groupName;

    /**
     * 是否打开消费开关
     * PullMessageProcessor.processRequest拉取消息
     * 会通过此属性校验是否能够拉取消息
     */
    private boolean consumeEnable = true;

    /**
     * 没有用了？
     */
    private boolean consumeFromMinEnable = true;

    /**
     * 当前groupName是否允许广播消息
     * PullMessageProcessor.processRequest拉取消息
     * 会通过此属性校验
     */
    private boolean consumeBroadcastEnable = true;

    /**
     * retryQueueNums 表示重试Topic messageQueue的数量
     * 每一个Consumer分组都会在初始化的时候都会添加并订阅重试Topic
     * MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup())
     *
     */
    private int retryQueueNums = 1;

    /**
     * 最大重试消费的次数
     *Broker端通过校验判断，如果超过了最大重试消费次数则会将消息移至这里所说的死信队列
     *MixAll.getDLQTopic(groupName)
     */
    private int retryMaxTimes = 16;

    private long brokerId = MixAll.MASTER_ID;

    private long whichBrokerWhenConsumeSlowly = 1;

    private boolean notifyConsumerIdsChangedEnable = true;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public boolean isConsumeEnable() {
        return consumeEnable;
    }

    public void setConsumeEnable(boolean consumeEnable) {
        this.consumeEnable = consumeEnable;
    }

    public boolean isConsumeFromMinEnable() {
        return consumeFromMinEnable;
    }

    public void setConsumeFromMinEnable(boolean consumeFromMinEnable) {
        this.consumeFromMinEnable = consumeFromMinEnable;
    }

    public boolean isConsumeBroadcastEnable() {
        return consumeBroadcastEnable;
    }

    public void setConsumeBroadcastEnable(boolean consumeBroadcastEnable) {
        this.consumeBroadcastEnable = consumeBroadcastEnable;
    }

    public int getRetryQueueNums() {
        return retryQueueNums;
    }

    public void setRetryQueueNums(int retryQueueNums) {
        this.retryQueueNums = retryQueueNums;
    }

    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }

    public void setRetryMaxTimes(int retryMaxTimes) {
        this.retryMaxTimes = retryMaxTimes;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public long getWhichBrokerWhenConsumeSlowly() {
        return whichBrokerWhenConsumeSlowly;
    }

    public void setWhichBrokerWhenConsumeSlowly(long whichBrokerWhenConsumeSlowly) {
        this.whichBrokerWhenConsumeSlowly = whichBrokerWhenConsumeSlowly;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(final boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (brokerId ^ (brokerId >>> 32));
        result = prime * result + (consumeBroadcastEnable ? 1231 : 1237);
        result = prime * result + (consumeEnable ? 1231 : 1237);
        result = prime * result + (consumeFromMinEnable ? 1231 : 1237);
        result = prime * result + (notifyConsumerIdsChangedEnable ? 1231 : 1237);
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + retryMaxTimes;
        result = prime * result + retryQueueNums;
        result =
            prime * result + (int) (whichBrokerWhenConsumeSlowly ^ (whichBrokerWhenConsumeSlowly >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionGroupConfig other = (SubscriptionGroupConfig) obj;
        if (brokerId != other.brokerId)
            return false;
        if (consumeBroadcastEnable != other.consumeBroadcastEnable)
            return false;
        if (consumeEnable != other.consumeEnable)
            return false;
        if (consumeFromMinEnable != other.consumeFromMinEnable)
            return false;
        if (groupName == null) {
            if (other.groupName != null)
                return false;
        } else if (!groupName.equals(other.groupName))
            return false;
        if (retryMaxTimes != other.retryMaxTimes)
            return false;
        if (retryQueueNums != other.retryQueueNums)
            return false;
        if (whichBrokerWhenConsumeSlowly != other.whichBrokerWhenConsumeSlowly)
            return false;
        if (notifyConsumerIdsChangedEnable != other.notifyConsumerIdsChangedEnable)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SubscriptionGroupConfig [groupName=" + groupName + ", consumeEnable=" + consumeEnable
            + ", consumeFromMinEnable=" + consumeFromMinEnable + ", consumeBroadcastEnable="
            + consumeBroadcastEnable + ", retryQueueNums=" + retryQueueNums + ", retryMaxTimes="
            + retryMaxTimes + ", brokerId=" + brokerId + ", whichBrokerWhenConsumeSlowly="
            + whichBrokerWhenConsumeSlowly + ", notifyConsumerIdsChangedEnable="
            + notifyConsumerIdsChangedEnable + "]";
    }
}
