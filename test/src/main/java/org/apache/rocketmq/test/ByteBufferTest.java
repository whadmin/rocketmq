package org.apache.rocketmq.test;

import java.nio.ByteBuffer;

public class ByteBufferTest {

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        for (int i = 0; i < buffer.capacity(); i++) {
            buffer.put((byte)i);
        }
        buffer.flip();
        byte[] bytesContent = new byte[10];
        ByteBuffer buffer1=buffer.get(bytesContent,0,1);
        ByteBuffer buffer2=buffer.get(bytesContent,1,1);
        ByteBuffer buffer3=buffer.get(bytesContent,2,1);

        for(int i = 0; i < bytesContent.length; i++){
            System.out.println(bytesContent[i]);
        }

        System.out.println("=======================");
        for (int i = 0; i < buffer1.limit()-1; i++) {
            System.out.println(buffer1.get());
        }
        System.out.println("=======================");
        for (int i = 0; i < buffer2.limit(); i++) {
            System.out.println(buffer2.get());
        }
        System.out.println("=======================");
        for (int i = 0; i < buffer3.limit(); i++) {
            System.out.println(buffer3.get());
        }
//        System.out.println(buffer);
//        ByteBuffer buffer1=buffer.slice();
//        buffer.put((byte)0);
//        buffer.put((byte)1);
//        System.out.println(buffer);
    }
}
