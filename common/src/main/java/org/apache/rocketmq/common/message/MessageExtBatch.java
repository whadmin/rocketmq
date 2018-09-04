/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.message;

import java.nio.ByteBuffer;

public class MessageExtBatch extends MessageExt {

    private static final long serialVersionUID = -2353110995348498537L;

    /**
     * 获取消息body字节数组写入到缓冲区中
     * 当消息为MessageExtBatch 我们会把每个消息encodeMessage(message)字节数组累加并返回
     * List<Message> messages = new ArrayList<>();
     * messageExtBatch.setBody(MessageDecoder.encodeMessages(messages));
     * @return
     */
    public ByteBuffer wrap() {
        assert getBody() != null;
        //将一个字节数组包装到缓冲区中。
        return ByteBuffer.wrap(getBody(), 0, getBody().length);
    }

    private ByteBuffer encodedBuff;

    public ByteBuffer getEncodedBuff() {
        return encodedBuff;
    }

    public void setEncodedBuff(ByteBuffer encodedBuff) {
        this.encodedBuff = encodedBuff;
    }
}
