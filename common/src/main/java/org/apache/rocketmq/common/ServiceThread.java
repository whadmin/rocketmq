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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * rocketmq的线程服务基类
 */
public abstract class ServiceThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 关闭服务线程同步等待服务线程中止等待时间
     */
    private static final long JOIN_TIME = 90 * 1000;
    /**
     * 服务执行工作线程
     */
    protected final Thread thread;
    /**
     * 线程同步工具,用来同步阻塞执行工作线程每次执行成功后等待10秒下次执行，此类扩展了CountDownLatch,增强了一个重置的线程阻塞状态的方法
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 通知工作线程是否有新的请求任务
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    /**
     * 服务是否停止
     */
    protected volatile boolean stopped = false;


    /**
     * 构造
     */
    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }



    /**
     * 启动服务
     */
    public void start() {
        this.thread.start();
    }

    /**
     * 关闭服务
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 关闭服务【会等待任务执行完毕返回】
     * @param interrupt 标记工作线程状态中止
     */
    public void shutdown(final boolean interrupt) {
        //标记服务已经中止
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        //如果服务还有新的通知释放同步锁的等待
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            //标记当前服务线程中止
            if (interrupt) {
                this.thread.interrupt();
            }

            //stopped = true 服务工作线程会退出循环，关闭服务线程等待服务工作线程中止
            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long eclipseTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }


    /**
     * 中止服务
     */
    public void stop() {
        this.stop(false);
    }

    /**
     * 中止服务【不会等待任务执行完毕返回】
     * @param interrupt
     */
    public void stop(final boolean interrupt) {
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    /**
     * 标记服务以及中止
     */
    public void makeStop() {
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }


    /**
     * 如果收到通知，释放等待同步锁
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 工作线程执行完成一次，如果没有新的通知会阻塞10秒
     * @param interval
     */
    protected void waitForRunning(long interval) {
        //如果有新的通知则返回不在等待
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //重置同步锁,同步状态再次变成1，
        waitPoint.reset();

        try {
            //同步等待10秒
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public abstract String getServiceName();

    public long getJointime() {
        return JOIN_TIME;
    }
}
