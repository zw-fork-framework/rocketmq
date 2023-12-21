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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerStartup {

    public static Logger log;
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();  // 磁盘存储空间达到预警线. 禁止写入,并清理，默认90%

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {
            controller.start();    //===> Broker是如何注册到NameServer的

            String tip = String.format("The broker[%s, %s] boot success. serializeType=%s",
                controller.getBrokerConfig().getBrokerName(), controller.getBrokerAddr(),
                RemotingCommand.getSerializeTypeConfigInThisServer());

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    public static BrokerController buildBrokerController(String[] args) throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        final BrokerConfig brokerConfig = new BrokerConfig();    // Broker核心配置类
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();   // Netty服务器配置
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();   // Netty客户端配置TLS
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();   // Broker 消息配置类
        nettyServerConfig.setListenPort(10911);   // 设置Netty服务器监听端口为 10911
        messageStoreConfig.setHaListenPort(0);

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        Properties properties = null;
        if (commandLine.hasOption('c')) { // 读取broker.conf配置文件，解析为BrokerConfig、 nettyServerConfig、nettyClientConfig、messageStoreConfig对象
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                CONFIG_FILE_HELPER.setFile(file);
                BrokerPathConfigHelper.setBrokerConfigPath(file);
                properties = CONFIG_FILE_HELPER.loadConfig();
            }
        }

        if (properties != null) {
            properties2SystemEnv(properties);
            MixAll.properties2Object(properties, brokerConfig);
            MixAll.properties2Object(properties, nettyServerConfig);
            MixAll.properties2Object(properties, nettyClientConfig);
            MixAll.properties2Object(properties, messageStoreConfig);
        }

        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
        if (null == brokerConfig.getRocketmqHome()) { // 检查ROCKETMQ_HOME环境变量， 如果没有直接退出
            System.out.printf("Please set the %s variable in your environment " +
                "to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // Validate namesrvAddr
        String namesrvAddr = brokerConfig.getNamesrvAddr(); // 获取Nameserver地址，;分割解析为数组 因为Nameserver可能为集群，所以有多个
        if (StringUtils.isNotBlank(namesrvAddr)) {
            try {
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    NetworkUtil.string2SocketAddress(addr);
                }
            } catch (Exception e) {
                System.out.printf("The Name Server Address[%s] illegal, please set it as follows, " +
                        "\"127.0.0.1:9876;192.168.0.1:9876\"%n", namesrvAddr);
                System.exit(-3);
            }
        }

        if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {  // broker是否为 slave broker 如果是 则将 消息占用内存百度分 减10%, 默认40%,超过内存的消息将置换出内存
            int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
            messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
        }

        // Set broker role according to ha config
        if (!brokerConfig.isEnableControllerMode()) {
            switch (messageStoreConfig.getBrokerRole()) { // 判断Broker角色，作相应的处理 master有两种同步消息方式
                // 异步复制:生产者写入消息到Master后无需等待消息复制到slave即可返回，消息的复制由旁路线程进行异步复制
                case ASYNC_MASTER:
                /*  同步复制的方式，表现出来的是类似同步双写的策略。即Master写入完消息之后，需要等待Slave的复制成功。
                    注，这里只需要有一个Slave复制成功并成功应答即算成功，所以在这种模式下，如果有3个Slave，当生产者获得SEND_OK的应答时，
                    代表消息已经达到Maser和一个Slave（注：这里并不代表已经持久化到磁盘，而只能证明肯定到了PageCache，
                    是否能刷到磁盘取决于刷盘策略是同步刷盘还是异步刷盘），而还有两个Slave实际上是无法保证的，
                    并且这里也不支持配置，即不支持如“”同步半数以上”之类的设置 */
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                /**
                 * 消息发送的状态除了SEND_OK外，还会多出以下的状态：
                 * FLUSH_SLAVE_TIMEOUT ： 同步到slave等待超时，即一直等Slave上报同步的进度，但过了超时时间都没有成功没有同步完。
                 * SLAVE_NOT_AVAILABLE：当前没有可用的Slave。注：如果slave落后master实在太多，
                 * 那个slave也会认为是暂时不可用的slave，直到它同步到接近的范围为止，
                 * 这个不可用的阈值由broker配置haSlaveFallbehindMax（默认是1024 * 1024 * 256）决定
                 */
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= MixAll.MASTER_ID) {
                        System.out.printf("Slave's brokerId must be > 0%n");
                        System.exit(-3);
                    }
                    break;
                default:
                    break;
            }
        }

        if (messageStoreConfig.isEnableDLegerCommitLog()) {  // 是否基于dleger技术管理主从同步和CommitLog
            brokerConfig.setBrokerId(-1);
        }

		 // 设置HA监听端口
        if (brokerConfig.isEnableControllerMode() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.out.printf("The config enableControllerMode and enableDLegerCommitLog cannot both be true.%n");
            System.exit(-4);
        }

        if (messageStoreConfig.getHaListenPort() <= 0) {
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
        }

        brokerConfig.setInBrokerContainer(false);

        System.setProperty("brokerLogDir", "");
        if (brokerConfig.isIsolateLogEnable()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId());
        }
        if (brokerConfig.isIsolateLogEnable() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + messageStoreConfig.getdLegerSelfId());
        }

        if (commandLine.hasOption('p')) { // 打印启动参数信息
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

        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        MixAll.printObjectProperties(log, brokerConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        MixAll.printObjectProperties(log, nettyClientConfig);
        MixAll.printObjectProperties(log, messageStoreConfig);

		 // 基于上面的配置参数 创建 BrokerController
        final BrokerController controller = new BrokerController(
            brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        // Remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    public static Runnable buildShutdownHook(BrokerController brokerController) {
        return new Runnable() {
            private volatile boolean hasShutdown = false;
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        brokerController.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    public static BrokerController createBrokerController(String[] args) {
        try {
            BrokerController controller = buildBrokerController(args);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
			// 钩子线程 JVM退出时打印消费时间
            Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(controller)));
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

    public static class SystemConfigFileHelper {
        private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigFileHelper.class);

        private String file;

        public SystemConfigFileHelper() {
        }

        public Properties loadConfig() throws Exception {
            InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
            Properties properties = new Properties();
            properties.load(in);
            in.close();
            return properties;
        }

        public void update(Properties properties) throws Exception {
            LOGGER.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }
}
