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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndexFile 用来记录索引信息
 *
 * 结构如下   indexHeader + SlotTable + Index Linked List 顺序存储
 *
 * indexHeader  表示索引头部信息 长度固定为40个字节  记录索引总体统计信息
 *
   beginTimestamp  8 IndexFile创建时间
   endTimestamp    8 IndexFile最后更新时间
   beginPhyOffset  8 存储消息初始物理偏移
   endPhyOffset    8 存储消息最大物理偏移
   indexCount      4 记录IndexFile 消息数量（每次添加+1）,同时作为消息在索引链表的下标
   hashSlotCount   4 记录IndexFile 被占用槽的数量

 * SlotTable    表示索引的槽表,长度固定为   4个字节 *  500w（默认500W）
 *
 * 4个字节：刚好是一个int 用来记录索引链表下标,每一个消息创建索引时都会在链表中添加一个节点，我们可以通过下标找到在链表对应的节点
 * 我们会将当以当前key为索引的最后一条消息在索引链表中的下标设置到槽中。
 * 如果计算key所在槽位置：通过获取key的hash值(int)  keyHash % this.hashSlotNum 获得所在的槽位置
 * 500W： 表示可以同时支持500W不同的key创建索引
 *
 *
 * Index Linked List  表示索引链表,长度固定为20字节 * 2000w记录（2000w）
 *
 * 20个字节表示一条消息在索引链表中的结构,如下:
 *
 * keyHash: 4位，int值，key的hash值
 * phyOffset：8位，long值，索引消息commitLog偏移量endTimestamp
 * timeDiff: 4位，int值，索引存储的时间与IndexHeader的beginTimestamp时间差(这是为了节省空间)
 * slotValue:4位，int值，索引对应slot的上一条索引的下标(据此完成一条向老记录的链表)
 *
 * 2000w:可以为2000w条消息创建索引
 *
 * 每当创建的消息存在索引时会在创建如上结构的数据作为节点添加到链表中，并记录下其在链表中顺序存储的下标
 *
 *
 * message1[key1,phyOffset,timeDiff,0][下标0]   --->  message1[key2,phyOffset,timeDiff,0][下标1] --->  message3[key1,phyOffset,timeDiff,0][下标3] --->  message4[key2,phyOffset,timeDiff,1][下标4]
 *
 *
 * 如果根据key查询到消息
 *
 * 1  获取key的hash值(int)，转成绝对值,计算keyHash % this.hashSlotNum获得所在的槽下标，
 *
 * 2  通过槽位置计算获取在indexFile偏移坐标,通过偏移坐标获取以当前key为索引的最后一条消息在索引链表中的下标

 * 3  通过消息在索引链表中的下标获取消息链表数据在indexFile偏移坐标,获取偏移坐标得到消息存储在链表节点中的信息
 *
 * keyHash: 4位，int值，key的hash值
 * phyOffset：8位，long值，索引消息commitLog偏移量endTimestamp
 * timeDiff: 4位，int值，索引存储的时间与IndexHeader的beginTimestamp时间差(这是为了节省空间)
 * slotValue:4位，int值，索引对应slot的上一条索引的下标(据此完成一条向老记录的链表)
 *
 *
 *
 * 4  判断当前消息slotValue 获取上一条消息值到获取所有以key为索引的所有消息数据物理偏移坐标

 */
public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 每一个槽数据占据字节大小
     */
    private static int hashSlotSize = 4;
    /**
     * 每一个消息在索引链表中占据字节大小
     */
    private static int indexSize = 20;
    /**
     * 每一个key创建时在槽中初始值
     */
    private static int invalidIndex = 0;
    /**
     * hash槽的个数，默认500w
     */
    private final int hashSlotNum;
    /**
     * 最多记录的索引条数 默认2千万
     */
    private final int indexNum;
    /**
     * mappedFile
     */
    private final MappedFile mappedFile;
    /**
     * 没有用到
     */
    private final FileChannel fileChannel;
    /**
     * mappedByteBuffer
     */
    private final MappedByteBuffer mappedByteBuffer;
    /**
     * 索引头
     */
    private final IndexHeader indexHeader;


    /**
     * 创建一个IndexFile
     * @param fileName  文件名
     * @param hashSlotNum  hash槽的个数，默认5million
     * @param indexNum     最多记录的索引条数,默认2千万
     * @param endPhyOffset 开始物理偏移
     * @param endTimestamp 开始的时间戳
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        //设置物理偏移
        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        //设置时间
        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    /**
     * 获取index文件名称
     * @return
     */
    public String getFileName() {
        return this.mappedFile.getFileName();
    }


    /**
     * 加载IndexHeader，读取取byteBuffer的内容到内存中
     */
    public void load() {
        this.indexHeader.load();
    }

    /**
     * 刷盘将字节缓冲区的数据写入磁盘
     */
    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            //读取IndexHeader属性更新到byteBuffer
            this.indexHeader.updateByteBuffer();
            //将字节缓冲区的数据写入磁盘
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    /**
     * IndexFile 写入的索引数据是否已经写满
     * @return
     */
    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    /**
     * 销毁IndexFile
     * @param intervalForcibly
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 通过对一条消息的key构建索引
     * @param key   消息的key
     * @param phyOffset       消息的物理偏移
     * @param storeTimestamp  消息的存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        //校验 IndexFile文件存储的消息索引数量是否超过限制的最大数量
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            //获取key的hash值(int)，转成绝对值
            int keyHash = indexKeyHashMethod(key);
            //计算消息hash值对应的Slot槽位置索引标
            int slotPos = keyHash % this.hashSlotNum;
            //获取Slot槽位置在indexfile的偏移坐标
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                //获取卡槽中的数据校验
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                //计算获取timeDiff
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                timeDiff = timeDiff / 1000;
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                //当前消息索引数据添加到链表中在indexfile的偏移坐标
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                /**
                 * 将消息的索引数据，添加到链表中
                 */
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                //设置上一条以此key作为索引的消息，在消息索引链表中的下标设置到本次消息数据结构中
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                /**
                 * 将消息在链表中的索引（下标），添加,更新到对应的槽位中
                 */
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                //如果是添加的第一个消息设置beginPhyOffset,beginTimestamp
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                //更新indexHeader
                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 通过key和时间为条件查询消息物理偏移列表
     * @param phyOffsets  物理偏移列表,用于返回
     * @param key
     * @param maxNum      物理偏移列表最多保存记录大于则直接返回
     * @param begin       开始时间戳
     * @param end         结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            //获取key的hash值(int)，转成绝对值
            int keyHash = indexKeyHashMethod(key);
            //计算消息hash值对应的Slot槽位置索引标
            int slotPos = keyHash % this.hashSlotNum;
            //获取Slot槽位置在indexfile的偏移坐标
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }
                //获取卡槽中数据[当前key为索引的最后一条消息在索引链表中的下标]
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }
                //校验下标
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    //通过下标获取到最后以此key添加到索引链表节点，通过节点中slotValue值向前遍历，查找以此key为节点的消息
                    for (int nextIndexToRead = slotValue; ; ) {
                        //返回值不大于maxNum不在获取更多的消息了
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        //通过链表中的下标,计算获取消息索引数据添加到链表中在indexfile的偏移坐标
                        int absIndexPos =
                                IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                        + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        //获取消息物理偏移坐标
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        //获取消息修改时间，相对第一条创建消息
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        //上一条和自己同样key消息在链表中的下标
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);
                        //判断消息是否满足时间条件
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                                || prevIndexRead > this.indexHeader.getIndexCount()
                                || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }

    /**
     * 获取key的hash值(int)，转成绝对值
     * @param key
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    /**
     * [begin,end]时间与 [indexHeader.getBeginTimestamp(), this.indexHeader.getEndTimestamp()]时间有重叠
     * @param begin
     * @param end
     * @return
     */
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }


    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }
}
