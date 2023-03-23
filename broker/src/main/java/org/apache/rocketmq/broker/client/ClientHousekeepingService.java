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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;

/**
 * ClientHousekeepingService 是 Broker 的一个组件，用于监控和清理不活跃的客户端连接。
 * <p>
 * 它实现了 RocketMQ 的 ChannelEventListener 接口，可以监听客户端连接的事件，比如连接建立、关闭、异常、闲置等，然后对 Broker 中的生产者管理器、
 * 消费者管理器、过滤器服务器管理器进行清理操作。
 * <p>
 * 它会定期扫描这些管理器中的连接，将不活跃的连接清理掉，从而避免浪费 Broker 的资源，提高 Broker 的稳定性和性能。
 * <p>
 * invoke：NettyEventExecutor.run
 */
public class ClientHousekeepingService implements ChannelEventListener {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // 相互持有
    private final BrokerController brokerController;

    // 调度线程
    private ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ClientHousekeepingScheduledThread"));

    public ClientHousekeepingService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    // 定时扫描清理不活跃的连接
                    // 消费者、生产者、过滤服务
                    ClientHousekeepingService.this.scanExceptionChannel();
                } catch (Throwable e) {
                    log.error("Error occurred when scan not active client channels.", e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }

    /**
     * 扫描清理不活跃连接
     */
    private void scanExceptionChannel() {
        this.brokerController.getProducerManager().scanNotActiveChannel();
        this.brokerController.getConsumerManager().scanNotActiveChannel();
        this.brokerController.getFilterServerManager().scanNotActiveChannel();
    }

    /**
     * Broker在销毁时调用，清理资源
     */
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }

    /**
     * 连接关闭事件
     *
     * @param remoteAddr 通道地址
     * @param channel    netty channel
     */
    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }


    /**
     * 连接异常事件
     *
     * @param remoteAddr 通道地址
     * @param channel    netty channel
     */
    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }


    /**
     * 连接闲置（心跳异常）事件
     *
     * @param remoteAddr 通道地址
     * @param channel    netty channel
     */
    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }
}
