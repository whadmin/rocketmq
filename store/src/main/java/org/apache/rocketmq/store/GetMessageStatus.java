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

public enum GetMessageStatus {

    /**
     * 查找到消息
     *
     */
    FOUND,

    /**
     * 没有查询到匹配的消息
     */
    NO_MATCHED_MESSAGE,

    MESSAGE_WAS_REMOVING,

    /**
     * 查询消息物理偏移位置没有在ConsumeQueue文件队列中找到
     */
    OFFSET_FOUND_NULL,
    /**
     *  查询消息在ConsumeQueue索引offset等于大于ConsumeQueue存储最大索引
     */
    OFFSET_OVERFLOW_BADLY,

    /**
     *  查询消息在ConsumeQueue索引offset等于当前ConsumeQueue存储最大索引
     */
    OFFSET_OVERFLOW_ONE,

    /**
     * 查询消息在ConsumeQueue索引offset小于当前ConsumeQueue存储最小索引
     * 【ConsumeQueue 会订单清理过期数据】
     */
    OFFSET_TOO_SMALL,

    /**
     * 查询消息topic-queueId 对应ConsumeQueue不存在
     */
    NO_MATCHED_LOGIC_QUEUE,

    /**
     * 查找ConsumeQueue没有数据
     */
    NO_MESSAGE_IN_QUEUE,
}
