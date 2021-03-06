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
package org.apache.rocketmq.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    //Netty客户端配置对象
    private final NettyClientConfig nettyClientConfig;

    //netty客户端对象
    private final Bootstrap bootstrap = new Bootstrap();

    //netty客户端处理work线程池组
    private final EventLoopGroup eventLoopGroupWorker;

    //更新当前连接服务器地址独占锁
    private final Lock lockNamesrvChannel = new ReentrantLock();

    //创建,关闭连接独占锁
    private final Lock lockChannelTables = new ReentrantLock();

    //保存NettyRemotingClient的服务器连接对象集合Map  key 为服务器端的地址,value为连接服务器的连接对象ChannelWrapper
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();

    //客户端的对象连接服务器的地址的集合对象，客户端对象可以连接多个服务器
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();

    // 当前客户端正在连接的服务端的地址,默认地址
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();

    /**
     * 定时任务扫描正在未完结请求
     */
    private final Timer timer = new Timer("ClientHouseKeepingService", true);


    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());

    //针对NettyRequestProcessor业务处理类  默认异步执行线程池对象
    private final ExecutorService publicExecutor;

    //针对一次异步RPC调用，针对回调异步处理线程池。
    private ExecutorService callbackExecutor;

    //针对监听客户端IO事件 提供回调实现
    /**
     * 客户端 注册 NettyConnectManageHandler【ChannelDuplexHandler】针对不同IO事件通过。
     * 向父类中nettyEventExecutor事件线程池中putNettyEvent事件，nettyEventExecutor会根据不同的事件调用
     * ChannelEventListener 对应的回调函数。
     */
    private final ChannelEventListener channelEventListener;

    //netty 异步处理ChannelHandlerAdapter Nio线程池对象
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    //RPCHook可以对RPC请求发起方，请求响应方提供了前置后置的扩展回调操作。
    private RPCHook rpcHook;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }


    /**
     * 初始化客户端对象
     * @param nettyClientConfig
     * @param channelEventListener
     */
    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        //设置netty配置对象
        this.nettyClientConfig = nettyClientConfig;
        //设置监听netty 客户端IO事件监听 并做回调处理
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        //设置针对NettyRequestProcessor业务处理类  默认异步执行线程池对象
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        //设置netty客户端处理IO事件的工作线程池组
        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

        //设置netty连接安全对象
        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                log.info("SSL enabled for client");
            } catch (IOException e) {
                log.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();

        return Math.abs(r.nextInt() % 999) % 999;
    }


    /**
     * 启动客户端
     */
    @Override
    public void start() {
        //单独定义个netty线程池组负责异步处理 ChannelHandlerAdapter 中回调方法。减轻eventLoopGroupWorker线程池组的工作，不设置默认由eventLoopGroupWorker线程池处理
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            nettyClientConfig.getClientWorkerThreads(),
            new ThreadFactory() {

                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                }
            });

        //初始化netty 客户端对象Bootstrap
        //NettyEncoder 负责编码
        //NettyDecoder 负责解码
        //IdleStateHandler netty心跳
        //NettyConnectManageHandler IO时间监听器
        //NettyClientHandler 客户端业务处理类
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            log.info("Prepend SSL handler");
                        } else {
                            log.warn("Connections are insecure as SSLContext is null!");
                        }
                    }
                    pipeline.addLast(
                        defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });

        //客户端定时任务执行扫描responseTable
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        //启动监听
        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }
    }

    /**
     * 关闭客户端
     *
     * 1 关闭定时任务
     * 2 关闭channelTables 中客户端和服务端所有连接NioSocketChannel
     * 3 清空channelTables
     * 4 关闭netty处理IO事件的线程池组
     * 5 关闭事件处理线程池
     * 6 关闭业务处理线程池组
     * 7 关闭通用线程池
     *
     */
    @Override
    public void shutdown() {
        try {
            //关闭定时任务
            this.timer.cancel();
            //关闭channelTables 中客户端和服务端所有连接NioSocketChannel
            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(null, cw.getChannel());
            }
            //清空channelTables
            this.channelTables.clear();
            //关闭netty处理IO事件的线程池组
            this.eventLoopGroupWorker.shutdownGracefully();
            //关闭事件处理线程池
            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }
            //关闭事件处理线程池
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }
        // 关闭通用线程池
        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }


    /**
     * 关闭一个服务器的连接
     * @param addr  服务器的地址
     * @param channel  客户端和服务器连接NioSocketChannel
     */
    public void closeChannel(final String addr, final Channel channel) {
        //关闭和服务器的连接Channel 不能为null
        if (null == channel)
            return;

        //当服务器地址为null == addr 从NioSocketChannel获取远程服务器的地址
        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            //关闭连接需要通过lockChannelTables加锁
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    //从channelTables 通过服务器地址，获取连接对象ChannelWrapper
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    log.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);

                    //不存在则说明不存在针对这个服务器地址的连接
                    if (null == prevCW) {
                        log.info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }
                    //关闭的的连接和客户端保存的连接对象不一致
                    else if (prevCW.getChannel() != channel) {
                        log.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addrRemote);
                        removeItemFromTable = false;
                    }
                    //从channelTables删除此地址的连接对象
                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }
                    //关闭连接
                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    /**
     * 关闭一个服务器的连接
     * @param channel
     */
    public void closeChannel(final Channel channel) {
        //关闭和服务器的连接Channel 不能为null
        if (null == channel)
            return;

        try {
            //关闭连接需要通过lockChannelTables加锁
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    //从已连接的对象中寻找要关闭连接对象
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        log.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    //存在要关闭的连接对象
                    if (removeItemFromTable) {
                        //从channelTables 删除此连接
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        //关闭此连接
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }


    /**
     * 注册一个钩子实现类，在客户端请求服务端的前，和返回后调用对应的钩子方法
     * @param rpcHook
     */
    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }


    /**
     * 更新客户端连接的服务端地址集合对象
     * @param addrs
     */
    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                log.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);
            }
        }
    }


    /**
     * invokeSync 表示客户端向服务端发起普通请求
     *
     * @param addr  请求服务端地址  localhost:8888
     * @param request  netty 请求IO对象
     * @param timeoutMillis  超时时间
     * @return
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        //客户端针对指定地址的服务器发起连接获取NioSocketChannel
        final Channel channel = this.getAndCreateChannel(addr);
        //判断NioSocketChannel 是否连接到服务端
        if (channel != null && channel.isActive()) {
            try {
                //客户端在调用RPC请求前 调用 rpcHook.doBeforeRequest 前置处理
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);
                //客户端在调用RPC请求后 调用 rpcHook.doAfterResponse 后置处理
                if (this.rpcHook != null) {
                    this.rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                }
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                //当nettyClientConfig.isClientCloseSocketIfTimeout()设置为true 客户端发送数据超时，则关闭此NioSocketChannel连接
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeChannel(addr, channel);
                    log.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                throw e;
            }
        }
        //NioSocketChannel 连接被关闭
        else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 客户端异步请求服务器
     * @param addr   服务器地址
     * @param request   请求包对象
     * @param timeoutMillis   请求超时时间
     * @param invokeCallback  请求响应后回调函数
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        //客户端针对指定地址的服务器发起连接获取NioSocketChannel
        final Channel channel = this.getAndCreateChannel(addr);
        //判断NioSocketChannel 是否连接到服务端
        if (channel != null && channel.isActive()) {
            try {
                //客户端在调用RPC请求前 调用 rpcHook.doBeforeRequest 前置处理
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                //调用父类NettyRemotingAbstract invokeAsyncImpl方法实现异步RPC请求
                this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 客户端单项请求服务器
     * @param addr   服务器地址
     * @param request   请求包对象
     * @param timeoutMillis 请求超时时间
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        //客户端针对指定地址的服务器发起连接获取NioSocketChannel
        final Channel channel = this.getAndCreateChannel(addr);
        //判断NioSocketChannel 是否连接到服务端
        if (channel != null && channel.isActive()) {
            try {
                //客户端在调用RPC请求前 调用 rpcHook.doBeforeRequest 前置处理
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                //调用父类NettyRemotingAbstract invokeOnewayImpl方法实现单项RPC请求
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * @param addr 请求服务端地址 localhost:8888
     * @return  服务端连接NioSocketChannel
     * @throws InterruptedException
     */
    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        //客户端连接服务端时,并没有明确传递要连接服务器地址时,客户端从本地缓存连接过的服务器地址集合对象namesrvAddrList中选举出一个默认的服务端地址设置也可以认为是当前连接的地址到对象namesrvAddrChoosed,并对此地址服务器重新连接，返回NioSocketChannel.
        if (null == addr)
            return getAndCreateNameserverChannel();

        //判断连接的地址，是否存在于channelTables中，每当客户端和服务器连接都会把服务器地址作为key,连接对象ChannelWrapper（NioSocketChannel对象的封装）作为value保存到channelTables中
        //如果存且连接对象ChannelWrapper正常直接返回
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
        //不存在于channelTables中，获取连接已经中断，则需要调用createChannel重新对addr地址的服务器发起连接。
        return this.createChannel(addr);
    }


    /**
     * 客户端连接服务端时,并没有明确传递要连接服务器地址时,
     * 客户端从本地缓存连接过的服务器地址集合对象namesrvAddrList中选举出一个默认的服务端地址设置到对象namesrvAddrChoosed,
     * 并对此地址服务器重新连接，返回NioSocketChannel.
     *
     * @return 服务端连接NioSocketChannel
     * @throws InterruptedException
     */
    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        //从namesrvAddrChoosed 获取默认的服务器的地址。
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            //从channelTables Map对象中中获取，这个默认地址的连接对象的ChannelWrapper（NioSocketChannel对象的封装）
            ChannelWrapper cw = this.channelTables.get(addr);
            //判断默认地址的连接对象的ChannelWrapper中NioSocketChannel连接是否正常。如果ChannelWrapper中NioSocketChannel连接正常返回当前连接的NioSocketChannel.
            if (cw != null && cw.isOK()) {
                return cw.getChannel();
            }
        }

        //如果客户端还没有设置默认服务器地址，则需要客户端从本地缓存连接过的服务器地址集合对象namesrvAddrList中选举出一个默认的服务端地址.
        final List<String> addrList = this.namesrvAddrList.get();
        //选举过程中需要加锁防止其他线程干扰.
        if (this.lockNamesrvChannel.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                //在校验下namesrvAddrChoosed中是否存在默认地址，并连接正常
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }
                //通过namesrvIndex计数器累加  % addrList.size() 求得一个连接过的地址，
                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();
                        String newAddr = addrList.get(index);
                        //设置选举出来了服务地址为默认连接的服务器地址
                        this.namesrvAddrChoosed.set(newAddr);
                        log.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        //针对默认的服务器地址进行重新连接
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null)
                            return channelNew;
                    }
                }
            } catch (Exception e) {
                log.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.lockNamesrvChannel.unlock();
            }
        } else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    /**
     * 创建一个连接
     * @param addr
     * @return
     * @throws InterruptedException
     */
    private Channel createChannel(final String addr) throws InterruptedException {
        //判断连接的地址，是否存在于channelTables中，每当客户端和服务器连接都会把服务器地址作为key,连接对象ChannelWrapper（NioSocketChannel对象的封装）作为value保存到channelTables中
        //如果存且连接对象ChannelWrapper，且此连接NioSocketChannel.通讯正常。说明客户端已经存在了和服务器连接不需要在重新连接。直接返回。
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            cw.getChannel().close();
            channelTables.remove(addr);
        }
        //连接过程中需要加锁防止其他线程干扰.
        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                //是否需要创建一个新的连接
                boolean createNewConnection;
                //从channelTables中获取该地址对应的ChannelWrapper连接对象
                cw = this.channelTables.get(addr);
                //cw!=null 说明客户端和此地址服务器客户端连接过
                if (cw != null) {
                    //cw.isOK() 表示存在一个和此地址连接正常的hannelWrapper连接对象，在此我们不会返回此连接，我们会关闭此连接通道，重新连接服务器
                    if (cw.isOK()) {
                        //关闭此连接
                        cw.getChannel().close();
                        //从channelTables中删除此地址的连接
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                    //!cw.getChannelFuture().isDone() 表示客户端发起连接服务端一直没有响应
                    else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    }
                    //连接失败
                    else {
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }
                //发起一个新连接将连接ChannelFuture对象设置到ChannelWrapper对象属性中，channelTables中添加新连接地址和连接对象ChannelWrapper
                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            //获取连接结果对象 ChannelFuture
            ChannelFuture channelFuture = cw.getChannelFuture();
            //等待nettyClientConfig.getConnectTimeoutMillis()时间，在等待时间内连接完成，返回此连接。否则连接失败，
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                //判断连接是否完成。完成则返回
                if (cw.isOK()) {
                    log.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString(), channelFuture.cause());
                }
            } else {
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                        channelFuture.toString());
            }
        }

        return null;
    }

    /**
     * 注册一个请求业务处理NettyRequestProcessor类，每一个requestCode，都会有一个NettyRequestProcessor。
     * 客户端使用Netty NettyClientHandler【SimpleChannelInboundHandler】 处理IO事件业务操作会通过解析 RPC请求包对象RemotingCommand
     * 获取RemotingCommand 类型  REQUEST_COMMAND 或者 RESPONSE_COMMAND 如果是REQUEST_COMMAND 会通过RemotingCommand 请求code使用对应的ettyRequestProcessor
     *
     * @param requestCode
     * @param processor
     * @param executor
     */
    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public RPCHook getRPCHook() {
        return this.rpcHook;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
