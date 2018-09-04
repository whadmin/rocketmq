package org.apache.rocketmq.test;

import java.nio.ByteBuffer;

public class ByteBufferTest {

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        for (int i = 0; i < buffer.capacity(); i++) {
            buffer.put((byte) i);
        }
        buffer.flip();
        buffer.get();
        buffer.get();
        buffer.get();
        buffer.put((byte) 11);

        for (int i = 0; i < buffer.limit() - buffer.position(); i++) {
            System.out.println(buffer.get());
        }


//        buffer.flip();
//        byte[] bytesContent = new byte[10];
//        ByteBuffer buffer1=buffer.get(bytesContent,0,1);
//        ByteBuffer buffer2=buffer.get(bytesContent,1,1);
//        ByteBuffer buffer3=buffer.get(bytesContent,2,1);
//
//        for(int i = 0; i < bytesContent.length; i++){
//            System.out.println(bytesContent[i]);
//        }
//
//        System.out.println("=======================");
//        for (int i = 0; i < buffer1.limit()-buffer1.position(); i++) {
//            System.out.println(buffer1.get());
//        }

//        System.out.println(buffer);
//        ByteBuffer buffer1=buffer.slice();
//        buffer.put((byte)0);
//        buffer.put((byte)1);
//        System.out.println(buffer);
    }
}
