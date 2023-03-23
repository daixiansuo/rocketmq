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
package org.apache.rocketmq.broker.out;

import com.google.common.collect.Lists;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;


/**
 * 主要封装 broker 向 nameserver 发送请求处理。注册、注销等等。
 * 其次还有 broker 与 broker 之间的数据查询 （主从同步）。
 */
public class BrokerOuterAPI {


    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // 远程通信客户端
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing(MixAll.getWSAddr());
    // nameserver 地址
    private String nameSrvAddr = null;
    // 是 Broker 线程池，用于处理与其他模块的通信请求，其核心线程数为 4，最大线程数为 10，空闲线程最大存活时间为 1 分钟，任务队列长度为 32
    private BrokerFixedThreadPoolExecutor brokerOuterExecutor = new BrokerFixedThreadPoolExecutor(4, 10, 1, TimeUnit.MINUTES,
            new ArrayBlockingQueue<Runnable>(32), new ThreadFactoryImpl("brokerOutApi_thread_", true));

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig, RPCHook rpcHook) {
        // 创建远程通信客户端，底层是netty
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingClient.registerRPCHook(rpcHook);
    }

    public void start() {
        // 启动
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.brokerOuterExecutor.shutdown();
    }


    /**
     * 提取 nameserver 地址
     *
     * @return String
     */
    public String fetchNameServerAddr() {
        try {
            // 提取nameserver地址
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                // 不相同，则更新
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    // 更新 NettyRemotingClient 维护的 nameserver地址
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            lst.add(addr);
        }

        this.remotingClient.updateNameServerAddressList(lst);
    }


    /**
     * 向所有 nameserver 注册 broker 信息
     *
     * @param clusterName        集群名称
     * @param brokerAddr         broker地址
     * @param brokerName         broker名称
     * @param brokerId           borkerId
     * @param haServerAddr       高可用地址
     * @param topicConfigWrapper 主题配置
     * @param filterServerList   过滤服务
     * @param oneway             是否单向
     * @param timeoutMills       超时时间
     * @param compressed         压缩
     * @return List<RegisterBrokerResult>
     */
    public List<RegisterBrokerResult> registerBrokerAll(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final String haServerAddr,
            final TopicConfigSerializeWrapper topicConfigWrapper,
            final List<String> filterServerList,
            final boolean oneway,
            final int timeoutMills,
            final boolean compressed) {

        // 存储注册结果
        final List<RegisterBrokerResult> registerBrokerResultList = Lists.newArrayList();
        // 获取所有 nameserver 地址
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        // 遍历向每个 nameserver 注册
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {

            // 请求头信息
            final RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setBrokerId(brokerId);
            requestHeader.setBrokerName(brokerName);
            requestHeader.setClusterName(clusterName);
            requestHeader.setHaServerAddr(haServerAddr);
            requestHeader.setCompressed(compressed);

            // 请求体信息
            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
            requestBody.setFilterServerList(filterServerList);
            // 编码
            final byte[] body = requestBody.encode(compressed);
            final int bodyCrc32 = UtilAll.crc32(body);
            requestHeader.setBodyCrc32(bodyCrc32);

            // 并发计数
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 发送注册请求（其实就是心跳请求）
                            RegisterBrokerResult result = registerBroker(namesrvAddr, oneway, timeoutMills, requestHeader, body);
                            if (result != null) {
                                registerBrokerResultList.add(result);
                            }

                            log.info("register broker[{}]to name server {} OK", brokerId, namesrvAddr);
                        } catch (Exception e) {
                            log.warn("registerBroker Exception, {}", namesrvAddr, e);
                        } finally {
                            // 请求结束，计数减一
                            countDownLatch.countDown();
                        }
                    }
                });
            }

            try {
                // 阻塞等待所有注册任务结束，或者超时结束。
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
        }

        return registerBrokerResultList;
    }


    /**
     * 向 nameserver 注册
     *
     * @param namesrvAddr   nameserver地址
     * @param oneway        单向
     * @param timeoutMills  超时
     * @param requestHeader 请求头信息
     * @param body          请求体
     * @return RegisterBrokerResult
     */
    private RegisterBrokerResult registerBroker(
            final String namesrvAddr,
            final boolean oneway,
            final int timeoutMills,
            final RegisterBrokerRequestHeader requestHeader,
            final byte[] body
    ) throws RemotingCommandException, MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException {

        // 请求
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);
        request.setBody(body);

        // 单向请求
        if (oneway) {
            try {
                this.remotingClient.invokeOneway(namesrvAddr, request, timeoutMills);
            } catch (RemotingTooMuchRequestException e) {
                // Ignore
            }
            return null;
        }

        // 发送注册请求
        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
        assert response != null;
        switch (response.getCode()) {
            // 响应状态
            case ResponseCode.SUCCESS: {
                RegisterBrokerResponseHeader responseHeader =
                        (RegisterBrokerResponseHeader) response.decodeCommandCustomHeader(RegisterBrokerResponseHeader.class);
                RegisterBrokerResult result = new RegisterBrokerResult();
                // 更新主节点地址
                result.setMasterAddr(responseHeader.getMasterAddr());
                // 更新高可用地址
                result.setHaServerAddr(responseHeader.getHaServerAddr());
                if (response.getBody() != null) {
                    result.setKvTable(KVTable.decode(response.getBody(), KVTable.class));
                }
                return result;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 向所有 nameserver 注销
     *
     * @param clusterName 集群名称
     * @param brokerAddr  broker地址
     * @param brokerName  broker名称
     * @param brokerId    brokerId
     */
    public void unregisterBrokerAll(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId
    ) {
        // nameserver地址列表
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            // 遍历注销
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    // 注销
                    this.unregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId);
                    log.info("unregisterBroker OK, NamesrvAddr: {}", namesrvAddr);
                } catch (Exception e) {
                    log.warn("unregisterBroker Exception, {}", namesrvAddr, e);
                }
            }
        }
    }


    /**
     * 注销
     *
     * @param namesrvAddr nameserver地址
     * @param clusterName 集群名称
     * @param brokerAddr  broker地址
     * @param brokerName  broker名称
     * @param brokerId    brokerId
     */
    public void unregisterBroker(
            final String namesrvAddr,
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {

        // 请求信息
        UnRegisterBrokerRequestHeader requestHeader = new UnRegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);
        requestHeader.setBrokerId(brokerId);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setClusterName(clusterName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_BROKER, requestHeader);

        // 发送注销请求
        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 检查 Broker 是否需要向 NameServer 注册或更新其信息。
     *
     * @param clusterName        集群名称
     * @param brokerAddr         地址
     * @param brokerName         名称
     * @param brokerId           id
     * @param topicConfigWrapper opic 配置的序列化对象。
     * @param timeoutMills       超时时间
     * @return 最后方法返回一个 boolean 类型的列表，表示每个 NameServer 是否需要更新 Broker 信息。
     */
    public List<Boolean> needRegister(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final TopicConfigSerializeWrapper topicConfigWrapper,
            final int timeoutMills) {

        // 存储结果
        final List<Boolean> changedList = new CopyOnWriteArrayList<>();
        // nameserver地址列表
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {
            // 并发计数
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            // 遍历请求
            for (final String namesrvAddr : nameServerAddressList) {
                // 提交异步任务处理
                brokerOuterExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 查询数据版本 请求头
                            QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
                            requestHeader.setBrokerAddr(brokerAddr);
                            requestHeader.setBrokerId(brokerId);
                            requestHeader.setBrokerName(brokerName);
                            requestHeader.setClusterName(clusterName);
                            // 请求体信息
                            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_DATA_VERSION, requestHeader);
                            request.setBody(topicConfigWrapper.getDataVersion().encode());
                            // 查询当前broker的数据版本
                            RemotingCommand response = remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
                            DataVersion nameServerDataVersion = null;
                            Boolean changed = false;
                            switch (response.getCode()) {
                                case ResponseCode.SUCCESS: {
                                    QueryDataVersionResponseHeader queryDataVersionResponseHeader =
                                            (QueryDataVersionResponseHeader) response.decodeCommandCustomHeader(QueryDataVersionResponseHeader.class);
                                    changed = queryDataVersionResponseHeader.getChanged();
                                    byte[] body = response.getBody();
                                    if (body != null) {
                                        // 解码 - 数据版本
                                        nameServerDataVersion = DataVersion.decode(body, DataVersion.class);
                                        // 判断是否相同，不相同则发生变化
                                        if (!topicConfigWrapper.getDataVersion().equals(nameServerDataVersion)) {
                                            changed = true;
                                        }
                                    }
                                    // 添加结果集
                                    if (changed == null || changed) {
                                        changedList.add(Boolean.TRUE);
                                    }
                                }
                                default:
                                    break;
                            }
                            log.warn("Query data version from name server {} OK,changed {}, broker {},name server {}", namesrvAddr, changed, topicConfigWrapper.getDataVersion(), nameServerDataVersion == null ? "" : nameServerDataVersion);
                        } catch (Exception e) {
                            changedList.add(Boolean.TRUE);
                            log.error("Query data version from name server {}  Exception, {}", namesrvAddr, e);
                        } finally {
                            // 计数减一
                            countDownLatch.countDown();
                        }
                    }
                });

            }
            try {
                // 阻塞等待计数结束 或者超时结束
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("query dataversion from nameserver countDownLatch await Exception", e);
            }
        }
        return changedList;
    }


    /**
     * 获取指定 Broker 地址上的所有主题配置信息
     *
     * @param addr broker地址
     * @return 主题配置信息
     */
    public TopicConfigSerializeWrapper getAllTopicConfig(
            final String addr) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        // 向 Broker 发送请求，用于获取 Broker 上的所有 Topic 配置信息。
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(true, addr), request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 向指定的 Broker 发送请求，获取所有消费者的消费进度。
     *
     * @param addr broker地址
     * @return 消费进度
     */
    public ConsumerOffsetSerializeWrapper getAllConsumerOffset(
            final String addr) throws InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ConsumerOffsetSerializeWrapper.decode(response.getBody(), ConsumerOffsetSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * 向 Broker 发送请求，用于获取所有延迟消息的偏移量。
     *
     * @param addr broker地址
     * @return 偏移量
     */
    public String getAllDelayOffset(
            final String addr) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return new String(response.getBody(), MixAll.DEFAULT_CHARSET);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * 从 Broker 获取所有的订阅组配置信息。
     *
     * @param addr broker地址
     * @return 订阅组配置信息
     */
    public SubscriptionGroupWrapper getAllSubscriptionGroupConfig(
            final String addr) throws InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void registerRPCHook(RPCHook rpcHook) {
        remotingClient.registerRPCHook(rpcHook);
    }
}
