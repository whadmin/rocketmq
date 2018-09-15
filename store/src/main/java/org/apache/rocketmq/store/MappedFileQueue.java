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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述ConsumeQueue CommitLog Index 文件队列
 */
public class MappedFileQueue {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 一次最多删除的文件数量
     */
    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**
     * 文件队列的存储路径
     *
     */
    private final String storePath;

    /**
     * 一个mappedFile文件大小,见MessageStoreConfig.mapedFileSizeCommitLog，默认1G
     */
    private final int mappedFileSize;


    /**
     * mappedFiles列表
     */
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /**
     * 创建MappedFile的服务
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 刷盘flush的坐标地址
     */
    private long flushedWhere = 0;
    /**
     * 提交commit的坐标地址
     */
    private long committedWhere = 0;

    /**
     * 最近消息写入mappedFile时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 构造MappedFileQueue
     * @param storePath
     * @param mappedFileSize
     * @param allocateMappedFileService
     */
    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * 检查mappedFiles中，除了最后一个文件，其余每一个mappedFile的大小是否是mappedFileSize
     */
    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 加载MappedFileQueue
     *
     * 对于文件队列中已经写完数据MappedFile  设置更新对应ByteBuffer坐标位置到完结。
     *
     * 你可能会疑问为什么文件都写满了对应ByteBuffer没有同步更新呢？
     * 这是因为写入时我们没有使用文件对应的ByteBuffer，而是使用ByteBuffer.slice()生成分片字节缓存区ByteBuffer，将消息加入到生成分片字节缓存区ByteBuffer
     * 分片缓冲区和其生成的缓存区共享数据，但坐标相对独立。因而即使分片缓存区中写入数据,其文件对应的缓冲区还坐标依旧保持不变。这里我们将依旧写满的文件对应字节缓冲区同步下。
     * 参考 MappedFile.appendMessagesInner,
     *
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // 文件排序，这样每一个文件都能遍历到
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }

                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }


    /**
     * 关闭MappedFileQueue
     * @param intervalForcibly
     */
    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }




    /*********************** 获取MappedFile star  ***********************/
    /**
     * 获取修改时间在timestamp之后的第一个mappedFile,没有的话就返回最后一个mappedFile
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;
        //获取最后修改时间在timestamp之后的第一个mappedFile
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }
        //没有的返回最后一个mappedFile
        return (MappedFile) mfs[mfs.length - 1];
    }

    /**
     * 转化mappedFiles成为 object[]数组
     * @param reservedMappedFiles  mappedFiles.size() <= reservedMappedFiles 返回null ？为什么这么做
     * @return
     */
    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }


    /*********************** 清理MappedFile star  ***********************/
    /**
     * 清理MappedFileQueue文件队列中文件初始偏移坐标大于offset 脏文件
     * 并对offset坐标对应appedFile偏移坐标重置到offset % this.mappedFileSize
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }
        //从mappedFiles 清理掉 files
        this.deleteExpiredFile(willRemoveFiles);
    }


    /**
     * 删除mappedFiles集合所有mappedFile对应物理文件,并清空mappedFiles
     */
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }


    /**
     * 删除mappedFiles集合最后一个mappedFile对应物理文件，并从管理mappedFiles集合中最后一个mappedFile
     */
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            //销毁此mappedFile
            lastMappedFile.destroy(1000);
            //从mappedFiles清理最后一个mappedFile
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }


    /**
     * 销毁第一个mappedFile
     * @param intervalForcibly
     * @return
     */
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    /**
     * 根据expiredTime删除过期文件,返回删除文件的数量
     * @return
     */
    public int deleteExpiredFileByTime(final long expiredTime,
                                       final int deleteFilesInterval,
                                       final long intervalForcibly,
                                       final boolean cleanImmediately) {
        //获取所有MappedFile
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        //记录删除文件的数量
        int deleteCount = 0;
        //记录删除文件
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                //计算判断超时时间
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                //满足过期时间判断删除mappedFile
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    //删除mappedFile物理文件
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }
                        //每删除一个文件的时间间隔
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 删除mappedFiles集合中 mappedFiles
     * @param files
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }


    /*********************** 清理MappedFile end  ***********************/


    /*********************** 获取创建MappedFile star  ***********************/

    /**
     * 返回mappedFiles中最后一个mappedFile，
     * 如果mappedFiles为空,根据startOffset创建出来最新的mappedFile
     * 如果mappedFiles最后一个写满了，则创建出来最新的mappedFile
     * @param startOffset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 返回mappedFiles中最后一个mappedFile，
     * 如果mappedFiles为空,根据startOffset以及needCreate判断是否需要创建出来最新的mappedFile
     * 如果mappedFiles最后一个写满了，根据needCreate判断是否需要创建出来最新的mappedFile
     * @param startOffset
     * @param needCreate
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        //返回mappedFiles中最后一个mappedFile
        MappedFile mappedFileLast = getLastMappedFile();

        //如果最后一个mappedFiles为空，计算创建mappedFile的坐标
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        //如果最后一个mappedFiles不为空，且mappedFiles已经满了，计算新创建mappedFile的坐标
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        //需要新创建mappedFiles
        if (createOffset != -1 && needCreate) {
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath = this.storePath + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;


            //创建mappedFiles，可以创建有2中方式
            // 1 使用allocateMappedFileService，
            // 2 手动直接创建
            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            //添加到mappedFiles 集合中
            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    /**
     * 返回mappedFiles中最后一个mappedFile
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    /**
     * 通过offset偏移坐标获取在MappedFileQueue文件队列中从属mappedFile
     * @param offset
     * @return
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    /**
     * 获取MappedFileQueue文件队列中mappedFile
     * @param offset offset偏移位置
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile mappedFile = this.getFirstMappedFile();
            if (mappedFile != null) {
                int index = (int) ((offset / this.mappedFileSize) - (mappedFile.getFileFromOffset() / this.mappedFileSize));
                if (index < 0 || index >= this.mappedFiles.size()) {
                    LOG_ERROR.warn("Offset for {} not matched. Request offset: {}, index: {}, " +
                                    "mappedFileSize: {}, mappedFiles count: {}",
                            mappedFile,
                            offset,
                            index,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                }

                try {
                    return this.mappedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mappedFile;
                    }
                    LOG_ERROR.warn("findMappedFileByOffset failure. ", e);
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    /**
     * 获取文件队列中第一个mappedFile，不存在返回NULL
     * @return
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }


    /**
     * 获取mappedFiles列表对应对象数组
     * @return
     */
    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }
    /*********************** 获取创建MappedFile end  ***********************/


    /*********************** 获取设置MappedFileQueue偏移 star  ***********************/
    /**
     * 重置文件队列最大偏移位置 offset
     * 大于offset偏移坐标的文件删除
     * @param offset
     * @return
     */
    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 获取文件队列中存储最小的偏移坐标
     * @return
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取文件队列中存储最最大的偏移坐标【==最后一个mappedFile文件初始偏移坐标+该文件已写入的坐标偏移】
     * @return
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取最后一个MappedFile写入的坐标偏移，这里不考虑使用transientStorePool
     * @return
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    /**
     * 获取最后一个MappedFile wrote位置，到queue中记录的commit的位置之差
     * @return
     */
    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    /**
     * 获取最后一个MappedFile commit位置，到queue中记录的flush的位置之差
     * @return
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }


    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }


    /**
     * 此方法只给ConsumeQueue使用
     *
     * 获取ConsumeQueue文件队列中指定偏移坐标对应消息所在commlog物理偏移坐标
     * 读取文件队列中所有mappedFile应字节缓冲区 跳过unitSize字节【ConsumeQueue中第一条数据结构为空结构需要跳过】，从pos为 this.mappedFileSize - unitSize开始读取
     * 获取第一个long数据maxOffsetInLogicQueue【获取ConsumeQueue队列文件mappedFile中保存第一条消息数据对应commlog物理偏移坐标】,
     * 当maxOffsetInLogicQueue < offset删除此文件
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }
    /*********************** 获取设置MappedFileQueue偏移 end  ***********************/

    /**
     * 通过flushedWhere获取MappedFileQueue中最后一个MappedFile
     * 调用flush将mappedByteBuffer或fileChannel中数据写入磁盘
     * 并记录committedWhere,storeTimestamp
     * @param flushLeastPages  刷盘允许的 间隔大小  flushLeastPages * 4k
     * @return
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 通过flushedWhere获取MappedFileQueue中最后一个MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            //调用flush将mappedByteBuffer或fileChannel中数据写入磁盘
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            //如果flush成功了result会为false ?为什么？
            result = where == this.flushedWhere;
            //记录committedWhere,storeTimestamp
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                //设置消息更新时间
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 通过committedWhere获取MappedFileQueue中最后一个MappedFile
     * 调用commit将MappedFile.ByteBuffer字节缓冲区中的数据写入MappedFile.fileChannel
     * 并记录committedWhere
     * @param commitLeastPages
     * @return
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        //通过committedWhere获取MappedFileQueue中最后一个MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            //调用commit将MappedFile.ByteBuffer字节缓冲区中的数据写入MappedFile.fileChannel
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            //如果commit成功了result会为false ?为什么？
            result = where == this.committedWhere;
            //记录committedWhere
            this.committedWhere = where;
        }

        return result;
    }












    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
