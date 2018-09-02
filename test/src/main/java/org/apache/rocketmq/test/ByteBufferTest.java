package org.apache.rocketmq.test;

import java.nio.ByteBuffer;

public class ByteBufferTest {

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        for (int i = 0; i < buffer.capacity()-3; i++) {
            buffer.put((byte)i);
        }
        System.out.println(buffer);
        ByteBuffer buffer1=buffer.slice();
        buffer.put((byte)0);
        buffer.put((byte)1);
        System.out.println(buffer);
    }
}
