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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;

/**
 * 平均分配队列策略
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final Logger log = ClientLogger.getLog();

    /**
     * 返回分配给当前客户端ID消费的List<MessageQueue>
     * @param consumerGroup current consumer group   消费分组
     * @param currentCID    current consumer id      当前consumer客户端ID
     * @param mqAll         message queue set in current topic  消费分组中需要消费所有topic对应List<MessageQueue>
     * @param cidAll        consumer set in current consumer group  当前消费分组中客户端ID集合【已排序】
     * @return
     *
     * 1 获取当前当前consumer客户端ID 在cidAll中的位置 index
     *
     * 2 计算当前客户端能分配到的List<MessageQueue>个数
     *   如果  mqAll.size()【肉】 <= cidAll.size()【僧】 那么每一个consumer客户端最多只能分配一个MessageQueue   僧多肉少
     *   如果  mqAll.size()【肉】 > cidAll.size()【僧】  那么每一个consumer客户端至少可以分配到 mqAll.size() / cidAll.size()) 僧少肉多
     *        多余的MessageQueue=mqAll.size() % cidAll.size() 如何分配？能否分配到当前consumer客户端ID？
     *           每一个客户端ID 是有一定优先级别的 排名靠前的优先拿多的那一个consumer
     *            index 在[ 0, mod )其中则获得多余一个consumer
     *
     * 3 知道了当前consumer客户端ID可以分配多少个就要计算则需要知道从mqAll哪个开始
     *      如果index 在[ 0, mod )中强到多余一个consumer 那么开始位置为  index * averageSize 【比自己优先级别高的也一定拿到了】
     *      如果index 在(nod, mqAll.size() )中没有强到多余一个consumer   那么开始位置为  index * averageSize + mod
     *
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }
        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();

        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());

        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;


        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
