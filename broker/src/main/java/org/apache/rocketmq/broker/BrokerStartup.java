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
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static Logger log;

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {

            controller.start();

            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static BrokerController createBrokerController(String[] args) {
        /**
         * 设置版本号[rocketmq.remoting.version -> MQVersion.CURRENT_VERSION]
         */
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        /**
         * 设置netty socket发送缓存大小
         */
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }
        /**
         * 设置netty 接收缓存大小
         */
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            /**
             * 构造org.apache.commons.cli.Options
             * 并且添加-h -n参数
             * -h 打印帮助信息
             * -h 指定Name Server地址
             */
            Options options = ServerUtil.buildCommandlineOptions(new Options());

            /**
             * 使用org.apache.commons.cli.PosixParser解析器和构造Options命令模板解析args得到 commandLine
             * buildCommandlineOptions 在Options中添加-c -p -m参数
             * -c 指定Name Server配置文件
             * -p 打印配置信息
             * -m 打印导入配置信息
             */
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            /**
             * 初始化brokerConfig
             */
            final BrokerConfig brokerConfig = new BrokerConfig();
            /**
             * 初始化nettyServerConfig
             */
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            /**
             * 初始化nettyClientConfig
             */
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            /**
             * 初始化messageStoreConfig
             */
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            /**
             * 设置nettyServerConfig TLS
             */
            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));

            /**
             * 设置nettyServerConfig 监听端口,这里表示Broker监听接受消息的端口号
             */
            nettyServerConfig.setListenPort(10911);


            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            /**
             * 如果命令带有-c参数，则读取指定broker配置文件文件内容，转换成全局Properties
             * 通过反射，将Properties中的值赋值给brokerConfig、nettyServerConfig,nettyClientConfig,messageStoreConfig
             */
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    properties2SystemEnv(properties);
                    MixAll.properties2Object(properties, brokerConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    MixAll.properties2Object(properties, messageStoreConfig);

                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }

            /**
             * 解析命令行参数，并加载到brokerConfig配置中
             */
            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            /**
             * 判断ROCKETMQ_HOME不能为空
             */
            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                    + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            }

            /**
             * 校验namesrvAddr 格式
             */
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }

            /**
             * 根据BrokerRole的角色设置，设置校验BrokerId
             * 这里有点奇怪 ASYNC_MASTER 和 SYNC_MASTER  BrokerId都是 0
             * SLAVE 必须 > 0
             */
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            /**
             * 设置message同步监听端口,当前broker为master，其他slave通不过连接这个端口号同步消息，默认 10912
             */
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);

            /**
             * 如果命令带有-p -m 参数，则打印出brokerConfig、nettyServerConfig,nettyClientConfig,messageStoreConfig的属性
             */
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");

            if (commandLine.hasOption('p')) {
                Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig);
                MixAll.printObjectProperties(console, nettyServerConfig);
                MixAll.printObjectProperties(console, nettyClientConfig);
                MixAll.printObjectProperties(console, messageStoreConfig);
                System.exit(0);
            } else if (commandLine.hasOption('m')) {
                Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig, true);
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                MixAll.printObjectProperties(console, messageStoreConfig, true);
                System.exit(0);
            }

            /**
             * 打印brokerConfig、nettyServerConfig,nettyClientConfig,messageStoreConfig的属性
             */
            log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);

            /**
             * 构造BrokerController
             * 该类是broker的主要控制类
             */
            final BrokerController controller = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                nettyClientConfig,
                messageStoreConfig);

            /**
             * remember all configs to prevent discard
             * 将全局Properties的内容复制到NamesrvController.Configuration.allConfigs中
             */
            controller.getConfiguration().registerConfig(properties);

            /**
             * 初始化 BrokerController
             */
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            /**
             * 注册ShutdownHook
             */
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }


    /**
     * Options中添加-c -p参数
     * -c 指定broker 配置文件
     * -p 打印配置信息
     * -m 打印导入配置信息
     */
    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
