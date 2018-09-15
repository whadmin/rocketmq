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
package org.apache.rocketmq.store.index;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 索引文件头信息
 */
public class IndexHeader {

    /**
     * 索引头信息总大小
     */
    public static final int INDEX_HEADER_SIZE = 40;
    /**
     * 8位long类型，索引文件构建第一个索引的消息落在broker的时间
     */
    private static int beginTimestampIndex = 0;
    /**
     * 8位long类型，索引文件构建最后一个索引消息落broker时间
     */
    private static int endTimestampIndex = 8;
    /**
     * 8位long类型，索引文件构建第一个索引的消息commitLog偏移量
     */
    private static int beginPhyoffsetIndex = 16;
    /**
     *  8位long类型，索引文件构建最后一个索引消息commitLog偏移量
     */
    private static int endPhyoffsetIndex = 24;
    /**
     * 4位int类型，构建索引占用的槽位数(这个值貌似没有具体作用)
     */
    private static int hashSlotcountIndex = 32;
    /**
     * 4位int类型，索引文件中构建的索引个数
     */
    private static int indexCountIndex = 36;
    /**
     * 临时存储字节缓冲区
     */
    private final ByteBuffer byteBuffer;
    /**
     * 第一条消息构建索引时间
     */
    private AtomicLong beginTimestamp = new AtomicLong(0);
    /**
     * 最后一条消息构建索引时间
     */
    private AtomicLong endTimestamp = new AtomicLong(0);
    /**
     * 第一条构建索引消息物理偏移坐标
     */
    private AtomicLong beginPhyOffset = new AtomicLong(0);
    /**
     * 最后一条构建索引消息物理偏移坐标
     */
    private AtomicLong endPhyOffset = new AtomicLong(0);

    /**
     * 记录IndexFile 被占用槽的数量
     */
    private AtomicInteger hashSlotCount = new AtomicInteger(0);

    /**
     * 记录IndexFile 消息数量（每次添加+1）,同时作为消息在索引链表的下标
     */
    private AtomicInteger indexCount = new AtomicInteger(1);

    public IndexHeader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }


    /**
     * 读取byteBuffer的内容设置到IndexHeader
     */
    public void load() {
        this.beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
        this.endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
        this.beginPhyOffset.set(byteBuffer.getLong(beginPhyoffsetIndex));
        this.endPhyOffset.set(byteBuffer.getLong(endPhyoffsetIndex));

        this.hashSlotCount.set(byteBuffer.getInt(hashSlotcountIndex));
        this.indexCount.set(byteBuffer.getInt(indexCountIndex));

        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1);
        }
    }


    /**
     * 读取IndexHeader属性更新到byteBuffer
     */
    public void updateByteBuffer() {
        this.byteBuffer.putLong(beginTimestampIndex, this.beginTimestamp.get());
        this.byteBuffer.putLong(endTimestampIndex, this.endTimestamp.get());
        this.byteBuffer.putLong(beginPhyoffsetIndex, this.beginPhyOffset.get());
        this.byteBuffer.putLong(endPhyoffsetIndex, this.endPhyOffset.get());
        this.byteBuffer.putInt(hashSlotcountIndex, this.hashSlotCount.get());
        this.byteBuffer.putInt(indexCountIndex, this.indexCount.get());
    }

    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
    }

    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(endTimestampIndex, endTimestamp);
    }

    public long getBeginPhyOffset() {
        return beginPhyOffset.get();
    }

    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(beginPhyoffsetIndex, beginPhyOffset);
    }

    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }

    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(endPhyoffsetIndex, endPhyOffset);
    }

    public AtomicInteger getHashSlotCount() {
        return hashSlotCount;
    }

    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(hashSlotcountIndex, value);
    }

    public int getIndexCount() {
        return indexCount.get();
    }

    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(indexCountIndex, value);
    }
}
