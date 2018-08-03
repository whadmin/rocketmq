package org.apache.rocketmq.remoting;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class RemotingServerTest {
    private static RemotingServer remotingServer;
    private static RemotingClient remotingClient;

    /**
     * 创建createRemotingServer
     * @return
     * @throws InterruptedException
     */
    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
        //设置netty服务端配置
        RemotingServer remotingServer = new NettyRemotingServer(config);
        //注册一个请求码为0的请求处理器。返回一个RemotingCommand,在返回时
        remotingServer.registerProcessor(0, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                request.setRemark("Hi " + ctx.channel().remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, Executors.newCachedThreadPool());
        //启动服务器
        remotingServer.start();

        return remotingServer;
    }

    /**
     * 创建RemotingClient
     * @return
     */
    public static RemotingClient createRemotingClient() {
        return createRemotingClient(new NettyClientConfig());
    }

    /**
     * 创建RemotingClient 配置nettyClientConfig
     * @param nettyClientConfig
     * @return
     */
    public static RemotingClient createRemotingClient(NettyClientConfig nettyClientConfig) {
        RemotingClient client = new NettyRemotingClient(nettyClientConfig);
        client.start();
        return client;
    }

    @BeforeClass
    public static void setup() throws InterruptedException {
        remotingServer = createRemotingServer();
        remotingClient = createRemotingClient();
    }

    @AfterClass
    public static void destroy() {
        remotingClient.shutdown();
        remotingServer.shutdown();
    }

    /**
     * 同步调用
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    @Test
    public void testInvokeSync() throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        //发乎请求code 为0
        RemotingCommand request = RemotingCommand.createRequestCommand(0, requestHeader);
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 10000000 * 1);
        assertTrue(response != null);
        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(response.getExtFields()).hasSize(2);

    }

    /**
     * 单向请求
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTimeoutException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingSendRequestException
     */
    @Test
    public void testInvokeOneway() throws InterruptedException, RemotingConnectException,
        RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeOneway("localhost:8888", request, 10000000 * 3);
    }

    /**
     * 异步请求
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTimeoutException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingSendRequestException
     */
    @Test
    public void testInvokeAsync() throws InterruptedException, RemotingConnectException,
        RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        final CountDownLatch latch = new CountDownLatch(1);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeAsync("localhost:8888", request, 10000000 * 3, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                latch.countDown();
                assertTrue(responseFuture != null);
                assertThat(responseFuture.getResponseCommand().getLanguage()).isEqualTo(LanguageCode.JAVA);
                assertThat(responseFuture.getResponseCommand().getExtFields()).hasSize(2);
            }
        });
        latch.await();
    }
}

class RequestHeader implements CommandCustomHeader {
    @CFNullable
    private Integer count;

    @CFNullable
    private String messageTitle;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getMessageTitle() {
        return messageTitle;
    }

    public void setMessageTitle(String messageTitle) {
        this.messageTitle = messageTitle;
    }
}

