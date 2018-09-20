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
package org.apache.rocketmq.client.producer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;

/**
 * DefaultMQProducer 这个类是Producer客户端应用程序API路口类.
 * DefaultMQProducer 负责处理发送同步消息,单向消息,异步消息，三种方式的消息, 以及连续的消息是否保证顺序消息【指定MessageQueueSelector或MessageQueue】，是否批量消息
 * 1 客户端通过 下面4个步骤发完消息的发送
     * 创建 DefaultMQProducer defaultMQProducer=new DefaultMQProducer()
     * 配置 DefaultMQProducer.setXXX()
     * 初始化 producer.start()
     * 发送消息  producer.send(msg)
 * 2 DefaultMQProducer发送消息的核心实现交由DefaultMQProducerImpl去实现【Producer核心类】
 * 3 DefaultMQProducer是ClientConfig的子类（提供了针对Producer配置）
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * MQProducer内部内部核心实现类, DefaultMQProducer大多数功能都委托给其处理执行
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

    /**
     * 生产者组:聚合了所有完全相同角色的生产者实例
     */
    private String producerGroup;

    /**
     * 用来producer在发送消息时自动生成topic
     */
    private String createTopicKey = MixAll.DEFAULT_TOPIC;

    /**
     * 每个默认主题创建的队列数量
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * 发送消息超时时间
     */
    private int sendMsgTimeout = 3000;

    /**
     * 压缩消息体阈值，即大于4k的消息体将默认压缩。
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * 在同步模式下声明发送失败之前内部执行的最大重试次数。
     * 这可能会导致消息重复，这取决于应用程序开发人员要解决的问题。
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * 在声明在异步模式下发送失败之前，在内部执行的最大重试次数。
     * 这可能会导致消息重复，这取决于应用程序开发人员要解决的问题。
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * 指示是否重试另一个代理在内部发送失败。
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * 允许的最大消息大小(以字节为单位)。
     */
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * 默认构造函数。
     */
    public DefaultMQProducer() {
        this(MixAll.DEFAULT_PRODUCER_GROUP, null);
    }

    /**
     * 构造函数，构造时设置producerGroup和RPC钩子
     *
     * @param producerGroup Producer group
     * @param rpcHook       RPC钩子。
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    /**
     * 构造函数 构造时设置producerGroup
     *
     * @param producerGroup
     */
    public DefaultMQProducer(final String producerGroup) {
        this(producerGroup, null);
    }

    /**
     * 构造函数 构造时设置RPC钩子
     *
     * @param rpcHook RPC钩子
     */
    public DefaultMQProducer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    /**
     * 启动
     *
     * @throws MQClientException if there is any unexpected error.
     */
    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.start();
    }

    /**
     * 关闭
     */
    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
    }

    /**
     * 获取主题的消息队列，我们可以后续可以向指定MessageQueue发送/发布消息。
     *
     * @param topic Topic to fetch.
     * @return List of message queues readily to send messages to
     * @throws MQClientException if there is any client error.
     */
    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(topic);
    }

    /*********************  同步消息接口开始   *********************/
    /**
     * 以同步模式发送消息。, 仅当发送过程完全完成时，此方法才会返回。
     * <strong>警告：</ strong>此方法具有内部重试机制，即内部实现将在声明失败之前重试* {@link #retryTimesWhenSendFailed}次。, 因此，可能会将多条消息*传递给经纪人。, 应用程序开发人员可以解决潜在的重复问题。
     *
     * @param msg Message to send.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(
            Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg);
    }

    /**
     * 与{@link #send（Message）}相同 另外指定发送超时。
     *
     * @param msg     Message to send.
     * @param timeout send timeout. 设置超时时间
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, timeout);
    }


    /**
     * 与{@link #send（Message）}相同 另外指定了目标消息队列。
     *
     * @param msg Message to send.
     * @param mq  Target message queue.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq);
    }

    /**
     * 与{@link #send（Message）}相同 另外指定了目标消息队列和发送超时
     *
     * @param msg     Message to send.
     * @param mq      Target message queue.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq, timeout);
    }

    /**
     * 与 {@link #send(Message)} 相同 另外指定了MessageQueueSelector,用来选择路由MessageQueue
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg      Argument to work along with message queue selector.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }

    /**
     * 与 {@link #send(Message)} 相同 另外指定了MessageQueueSelector,用来选择路由MessageQueue 同时指定超时时间
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg      Argument to work along with message queue selector.
     * @param timeout  Send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }
    /*********************  同步消息接口结束   *********************/

    /*********************  异步消息接口开始   *********************/
    /**
     * 异步发送消息给代理。此方法立即返回。, 发送完成后，将执行<code> sendCallback </ code>
     * 与{@link #send（Message）}类似，内部实现可能会在声明发送失败之前重试最多* {@link #retryTimesWhenSendAsyncFailed}次，这可能会产生消息重复*并且应用程序开发人员可以解决此潜在问题。
     *
     * @param msg          Message to send.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg,
                     SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }

    /**
     * 与{@link #send（Message，SendCallback）}相同 另外指定发送超时。
     *
     * @param msg          message to send.
     * @param sendCallback Callback to execute.
     * @param timeout      send timeout.  设置超时时间
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }

    /**
     * 与{@link #send（Message，SendCallback）}相同 另外指定了目标消息队列
     *
     * @param msg          Message to send.
     * @param mq           Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback);
    }

    /**
     * 与{@link #send（Message，SendCallback）}相同 另外指定了目标消息队列和发送超时
     *
     * @param msg          Message to send.
     * @param mq           Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @param timeout      Send timeout.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback, timeout);
    }

    /**
     * 与{@link #send（Message，SendCallback）}相同 另外指定了MessageQueueSelector,用来选择路由MessageQueue
     *
     * @param msg          Message to send.
     * @param selector     Message selector through which to get target message queue.
     * @param arg          Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }

    /**
     * 与{@link #send（Message，SendCallback）}相同, 另外指定了MessageQueueSelector,用来选择路由MessageQueue,同时指定超时时间
     *
     * @param msg          Message to send.
     * @param selector     Message selector through which to get target message queue.
     * @param arg          Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @param timeout      Send timeout.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }

    /*********************  异步消息接口结束   *********************/

    /*********************  单向消息接口开始   *********************/
    /**
     * 发送单向消息，此方法在返回之前不会等待broker来自代理的*确认。, 显然，它具有最大的吞吐量但消息丢失的风险。
     *
     * @param msg Message to send.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg);
    }


    /**
     * 与{@link #sendOneway(Message)} 相同 另外指定了目标消息队列
     *
     * @param msg Message to send.
     * @param mq  Target message queue.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg,
                           MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, mq);
    }

    /**
     * 与{@link #sendOneway(Message)} 相同 另外指定了MessageQueueSelector,用来选择路由MessageQueue
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which to determine target message queue to deliver message
     * @param arg      Argument used along with message queue selector.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }

    /*********************   单向消息接口结束     ********************/


    /*********************  同步批量消息接口开始   ********************/
    @Override
    public SendResult send(
            Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs));
    }

    @Override
    public SendResult send(Collection<Message> msgs,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), timeout);
    }

    @Override
    public SendResult send(Collection<Message> msgs,
                           MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue,
                           long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                MessageClientIDSetter.setUniqID(message);
            }
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        return msgBatch;
    }

    /*********************  同步批量消息接口结束   *********************/

    /*********************  申请Topic接口开始     *********************/
    /**
     * 在broker上申请Topic,如果Broker  rautoCreateTopicEnable=false 我们在发送指定Topic消息前 必须先向Broker发出申请
     * 申请创建存储Topic类型的消息的MessageQueue
     *
     * @param key      accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    /**
     * 与{@link #createTopic(String key, String newTopic, int queueNum)} 相同,另外指定了同步类型
     *
     * @param key          accesskey
     * @param newTopic     topic name
     * @param queueNum     topic's queue number
     * @param topicSysFlag topic system flag
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }
    /*********************  申请Topic接口结束   *********************/


    /*********************  其他接口开始   *********************/
    /**
     * @param mq        Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return Consume queue offset.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(mq, timestamp);
    }

    /**
     * @param mq Instance of MessageQueue
     * @return maximum offset of the given consume queue.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(mq);
    }

    /**
     * @param mq Instance of MessageQueue
     * @return minimum offset of the given message queue.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(mq);
    }

    /**
     * @param mq Instance of MessageQueue
     * @return earliest message store time.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(mq);
    }

    /**
     * @param offsetMsgId message id
     * @return Message specified.
     * @throws MQBrokerException    if there is any broker error.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public MessageExt viewMessage(
            String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
    }

    /**
     * @param topic  message topic
     * @param key    message key index word
     * @param maxNum max message number
     * @param begin  from when
     * @param end    to when
     * @return QueryResult instance contains matched messages.
     * @throws MQClientException    if there is any client error.
     * @throws InterruptedException if the thread is interrupted.
     */
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(topic, key, maxNum, begin, end);
    }

    /**
     * @param topic Topic
     * @param msgId Message ID
     * @return Message specified.
     * @throws MQBrokerException    if there is any broker error.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public MessageExt viewMessage(String topic,
                                  String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageId oldMsgId = MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(topic, msgId);
    }
    /*********************  其他接口结束   *********************/

    /**
     * 注册一个线程池,异步处理 InvokeCallback
     * @param callbackExecutor the instance of Executor
     */
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.defaultMQProducerImpl.setCallbackExecutor(callbackExecutor);
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }

    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }

    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    /**
     * 此方法用于发送事务性消息,【不能使用,事务消息使用TransactionMQProducer】
     *
     * @param msg          Transactional message to send.
     * @param tranExecuter local transaction executor.
     * @param arg          Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
                                                          final Object arg)
            throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }
}
