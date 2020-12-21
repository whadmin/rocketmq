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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //池大小
    private final int poolSize;
    //池中推外缓冲区大小
    private final int fileSize;
    //池中推外缓冲区队列
    private final Deque<ByteBuffer> availableBuffers;
    //消息持久化配置
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        //设置消息持久化配置
        this.storeConfig = storeConfig;
        //设置池大小
        this.poolSize = storeConfig.getTransientStorePoolSize();
        //设置池中推外缓冲区大小
        this.fileSize = storeConfig.getMapedFileSizeCommitLog();
        //初始化池中推外缓冲区队列
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     *初始化堆外缓存池
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));
            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * 销毁堆外缓存池
     *
     */
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 归还一个堆外缓存区给堆外缓冲区池
     * @param byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 借一堆外缓存区从堆外缓冲区池
     * @return
     */
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    /**
     * 剩余可用的堆外缓冲区数量
     * @return
     */
    public int remainBufferNumbs() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
