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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.extend.MyDefaultAppendMessageCallback;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;


public class MappedFileTest {

    private static final Logger log = LoggerFactory.getLogger(MappedFileTest.class);
    private static final int MAPPED_FILE_SIZE = 1024 * 1024 * 4;

    private final String messageBody = "Hello";

    //使用MappedBuffer方式写入消息 MappedFile
    private MappedFile mappedFile;
    //创建使用TransientStorePool方式写入消息 MappedFile
    private MappedFile mappedFile2;
    //消息存储配置
    private MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
    //写入消息配置
    private MyDefaultAppendMessageCallback defaultAppendMessageCallback2 = new MyDefaultAppendMessageCallback(MAPPED_FILE_SIZE);


    @Before
    public void init() throws Exception {
        //创建使用MappedBuffer方式写入消息 MappedFile
        mappedFile = new MappedFile("target/MappedFileTest/000", MAPPED_FILE_SIZE);

        //设置堆外缓冲区池大小
        messageStoreConfig.setTransientStorePoolSize(5);
        //设置CommitLog文件夹中文件大小
        messageStoreConfig.setMapedFileSizeCommitLog(MAPPED_FILE_SIZE);
        //创建使用TransientStorePool方式写入消息 MappedFile
        mappedFile2 = new MappedFile("target/MappedFileTest2/000", MAPPED_FILE_SIZE, new TransientStorePool(messageStoreConfig));
    }

    /**
     * 测试MappedFile 不使用TransientStorePool 写入消息
     */
    @Test
    public void testSelectMappedBufferAppendMessage() throws IOException {
        //向MappedFile追加数据
        boolean result = mappedFile.appendMessage(messageBody.getBytes());
        assertThat(result).isTrue();

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        byte[] data = new byte[messageBody.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);

        assertThat(readString).isEqualTo(messageBody);

        mappedFile.shutdown(1000);
        assertThat(mappedFile.isAvailable()).isFalse();
        selectMappedBufferResult.release();
        assertThat(mappedFile.isCleanupOver()).isTrue();
        assertThat(mappedFile.destroy(1000)).isTrue();
    }


    /**
     * 测试MappedFile 不使用TransientStorePool 写入消息
     */
    @Test
    public void testSelectMappedBuffer() throws Exception {

        MessageExtBrokerInner inner = buildMessage();

        //向MapperFile追加消息,消息会写入，mappedByteBuffer（内存映射字节缓冲区），等待异步刷盘
        AppendMessageResult result = mappedFile.appendMessage(inner, defaultAppendMessageCallback2);
        System.out.println("状态：" + result.getStatus().name());
        System.out.println("消息id：" + result.getMsgId());
        System.out.println("获取消息大小：" + result.getWroteBytes());
        System.out.println("消息物理偏移：" + result.getWroteOffset());
        System.out.println("消息逻辑偏移：" + result.getLogicsOffset());
        System.out.println("消息生成时间：" + result.getStoreTimestamp());

        AppendMessageResult result2 = mappedFile.appendMessage(buildMessage(), defaultAppendMessageCallback2);
        System.out.println("状态：" + result2.getStatus().name());
        System.out.println("消息id：" + result2.getMsgId());
        System.out.println("获取消息大小：" + result2.getWroteBytes());
        System.out.println("消息物理偏移：" + result2.getWroteOffset());
        System.out.println("消息逻辑偏移：" + result2.getLogicsOffset());
        System.out.println("消息生成时间：" + result2.getStoreTimestamp());

        //将mappedByteBuffer中数据写入磁盘，
        mappedFile.flush(0);

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(readBodyStar(0, inner.getBody().length));
        byte[] data = new byte[messageBody.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);
        System.out.println("读取第一条消息body:" + readString);

        SelectMappedBufferResult selectMappedBufferResult2 = mappedFile.selectMappedBuffer(readBodyStar(inner.getBody().length, inner.getBody().length));
        byte[] data2 = new byte[messageBody.length()];
        selectMappedBufferResult2.getByteBuffer().get(data2);
        String readString2 = new String(data2);
        System.out.println("读取第二条消息body:" + readString2);
    }

    /**
     * 测试MappedFile 使用TransientStorePool 写入消息
     */
    @Test
    public void testSelectTransientStorePool() throws Exception {
        MessageExtBrokerInner inner = buildMessage();

        //向MapperFile追加消息,消息会写入，mappedByteBuffer（内存映射字节缓冲区），等待异步刷盘
        AppendMessageResult result = mappedFile2.appendMessage(inner, defaultAppendMessageCallback2);
        System.out.println("状态：" + result.getStatus().name());
        System.out.println("消息id：" + result.getMsgId());
        System.out.println("获取消息大小：" + result.getWroteBytes());
        System.out.println("消息物理偏移：" + result.getWroteOffset());
        System.out.println("消息逻辑偏移：" + result.getLogicsOffset());
        System.out.println("消息生成时间：" + result.getStoreTimestamp());

        AppendMessageResult result2 = mappedFile2.appendMessage(buildMessage(), defaultAppendMessageCallback2);
        System.out.println("状态：" + result2.getStatus().name());
        System.out.println("消息id：" + result2.getMsgId());
        System.out.println("获取消息大小：" + result2.getWroteBytes());
        System.out.println("消息物理偏移：" + result2.getWroteOffset());
        System.out.println("消息逻辑偏移：" + result2.getLogicsOffset());
        System.out.println("消息生成时间：" + result2.getStoreTimestamp());

        mappedFile.commit(0);

        mappedFile.flush(0);

        SelectMappedBufferResult selectMappedBufferResult = mappedFile2.selectMappedBuffer(readBodyStar(0, inner.getBody().length));
        byte[] data = new byte[messageBody.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);
        System.out.println("读取第一条消息body:" + readString);

        SelectMappedBufferResult selectMappedBufferResult2 = mappedFile2.selectMappedBuffer(readBodyStar(inner.getBody().length, inner.getBody().length));
        byte[] data2 = new byte[messageBody.length()];
        selectMappedBufferResult2.getByteBuffer().get(data2);
        String readString2 = new String(data2);
        System.out.println("读取第二条消息body:" + readString2);
    }

    /**
     * 创建消息
     */
    public MessageExtBrokerInner buildMessage() throws Exception {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic("TOPIC");
        msgInner.setQueueId(1);

        msgInner.setBody(messageBody.getBytes());
        msgInner.setFlag(0);
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties("UNIQ_KEY0AE0DC40298818B4AAC250FA21A60000WAITtrueTAGSTag1"));
        msgInner.setPropertiesString("UNIQ_KEY0AE0DC40298818B4AAC250FA21A60000WAITtrueTAGSTag1");
        msgInner.setBornTimestamp(System.currentTimeMillis());

        msgInner.setSysFlag(MessageSysFlag.TRANSACTION_NOT_TYPE);
        msgInner.setBornHost(new InetSocketAddress(InetAddress.getLocalHost(), 8123));
        msgInner.setStoreHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        msgInner.setReconsumeTimes(0);

        // 设置消息的生成时间
        msgInner.setStoreTimestamp(System.currentTimeMillis());
        // 设置邮件正文BODY CRC
        msgInner.setBodyCRC(UtilAll.crc32(msgInner.getBody()));
        return msgInner;
    }

    private static int readBodyStar(int offset, int topicLength) {
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
                + 4;
        return offset + msgLen;
    }

//    @After
//    public void destory() {
//        File file = new File("target/MappedFileTest/001");
//        UtilAll.deleteFile(file);
//    }
}
