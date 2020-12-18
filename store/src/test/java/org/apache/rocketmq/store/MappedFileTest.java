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
 * $Id: MappedFileTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;


public class MappedFileTest {

    private static final Logger log = LoggerFactory.getLogger(MappedFileTest.class);

    private final String storeMessage = "Once, there was a chance for me!";
    private AtomicInteger QueueId = new AtomicInteger(0);
    private final int QUEUE_TOTAL = 16;
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private MappedFile mappedFile;
    private DefaultAppendMessageCallback2 defaultAppendMessageCallback2=new DefaultAppendMessageCallback2(1024 * 1024 * 4);


    @Before
    public void init() throws Exception {
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        mappedFile = new MappedFile("target/MappedFileTest/001", 1024 * 64);
    }

    /**
     * 测试MappedFile
     * <p>
     * MappedFile选择使用通过内存映射ByteBuffer字节缓冲区完成文件操作
     */
    @Test
    public void testSelectMappedBufferAppendMessage() throws IOException {

        //向MappedFile追加数据
        boolean result = mappedFile.appendMessage(storeMessage.getBytes());
        assertThat(result).isTrue();

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        byte[] data = new byte[storeMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);

        assertThat(readString).isEqualTo(storeMessage);

        mappedFile.shutdown(1000);
        assertThat(mappedFile.isAvailable()).isFalse();
        selectMappedBufferResult.release();
        assertThat(mappedFile.isCleanupOver()).isTrue();
        assertThat(mappedFile.destroy(1000)).isTrue();
    }

    @After
    public void destory() {
        File file = new File("target/MappedFileTest/000");
        UtilAll.deleteFile(file);
    }

    @Test
    public void testSelectMappedBuffer() throws Exception {

        AppendMessageResult result = mappedFile.appendMessage(buildMessage(), defaultAppendMessageCallback2);
        mappedFile.flush(0);

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        byte[] data = new byte[storeMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);
        log.info(readString);

    }


    public MessageExtBrokerInner buildMessage() throws Exception {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic("TOPIC");
        msgInner.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);

        msgInner.setBody(storeMessage.getBytes());
        msgInner.setFlag(0);
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties("UNIQ_KEY0AE0DC40298818B4AAC250FA21A60000WAITtrueTAGSTag1"));
        msgInner.setPropertiesString("UNIQ_KEY0AE0DC40298818B4AAC250FA21A60000WAITtrueTAGSTag1");
        msgInner.setBornTimestamp(System.currentTimeMillis());

        msgInner.setSysFlag(MessageSysFlag.TRANSACTION_NOT_TYPE);
        msgInner.setBornHost(bornHost);
        msgInner.setStoreHost(storeHost);
        msgInner.setReconsumeTimes(0);

        // 设置消息的生成时间
        msgInner.setStoreTimestamp(System.currentTimeMillis());
        // 设置邮件正文BODY CRC
        msgInner.setBodyCRC(UtilAll.crc32(msgInner.getBody()));
        return msgInner;
    }

    public class DefaultAppendMessageCallback2 implements AppendMessageCallback {

        private final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

        // 文件在最小固定长度结束时为空
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // msgIdMemory字节缓冲区8个字段负责存储消息存储IP+端口+消息物理偏移坐标,用来计算消息id
        private final ByteBuffer msgIdMemory;
        // msgStoreItemMemory负责临时存在消息数据
        private final ByteBuffer msgStoreItemMemory;
        // hostHolder字节缓冲区8个字段负责存储消息存储IP+端口
        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);
        // 单个消息存储在文件中的最大大小，默认为512K 从defaultMessageStore.getMessageStoreConfig().getMaxMessageSize()获取
        private final int maxMessageSize;
        // 存储topicQueueTable key=Topic+"-"+queueId
        private final StringBuilder keyBuilder = new StringBuilder();

        private final StringBuilder msgIdBuilder = new StringBuilder();

        SystemClock systemClock = new SystemClock();

        DefaultAppendMessageCallback2(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        @Override
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBrokerInner msgInner) {
            // 计算消息物理偏移坐标
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 重置hostHolder字节缓冲区，hostHolder字节缓冲区8个字段负责存储消息存储IP+端口
            this.resetByteBuffer(hostHolder, 8);

            // 获取消息在队列中偏移坐标，如果部存在则初始化
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.topicQueueTable.get(key);
            // 不存在则初始化
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.topicQueueTable.put(key, queueOffset);
            }

            // 1 读取消息msgInner中的存储IP+端口写入hostHolder
            // 2 重置msgIdMemory字节缓冲区，msgIdMemory字节缓冲区8个字段负责存储消息存储IP+端口+消息物理偏移坐标
            // 3 计算msgIdMemory获取消息msgId
            String msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(hostHolder), wroteOffset);

            // 获取消息类型
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());

            //序列化消息propertiesString,并获取字节长度，同时校验propertiesString字节长度
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;
            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            //序列化消息Topic,并获取字节长度
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
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

            //校验总字节长度，如果当前MappedFile文件没办法存储当前消息
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                //重置 msgStoreItemMemory字节缓冲区
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 向msgStoreItemMemory 追加maxBlank长度
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 向msgStoreItemMemory 追加文件结束空MAGIC CODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 获取当前时间
                final long beginTimeMills = this.systemClock.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                // 4 返回AppendMessageResult 类型为END_OF_FILE 表示文件无法满足此消息写入，外部会创建一个新的MappedFile重新写入
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                        queueOffset, this.systemClock.now() - beginTimeMills);
            }

            // 重置 msgStoreItemMemory字节缓冲区
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

            final long beginTimeMills = this.systemClock.now();
            // msgStoreItemMemory 添加到MappedFile对应 byteBuffer
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            //构造返回结果
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                    msgInner.getStoreTimestamp(), queueOffset, this.systemClock.now() - beginTimeMills);

            return result;
        }

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBatch messageExtBatch) {
            return null;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }
    }

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

    public static class CommitLog {

        private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

        private final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;

        public final static int MESSAGE_MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;

        private static HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);

    }
}
