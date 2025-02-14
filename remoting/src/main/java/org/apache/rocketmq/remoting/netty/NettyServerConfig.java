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

public class NettyServerConfig implements Cloneable {

    // NameServer监听端口，该值在 nameserver 侧默认会被初始化为 9876； 在 broker 侧默认初始化为 10911
    private int listenPort = 8888;
    // Netty业务线程池线程个数
    private int serverWorkerThreads = 8;
    // Netty public任务线程池 线程个数。
    // Netty网络会根据业务类型创建不同的线程池，比如处理 消息发送、消息消费、心跳检测等。如果该业务类型(RequestCode)未 注册线程池，则由public线程池执行。
    // NettyRemotingServer 在使用这个变量时，会加以判断 默认初始化为 4 。
    private int serverCallbackExecutorThreads = 0;
    // I/O线程池线程个数。
    // 主要是 NameServer、Broker端解析请求、返回相应的线程个数。这类线程主 要用于处理网络请求，先解析请求包，然后转发到各个业务线程池完 成具体的业务操作，最后将结果返回给调用方。
    private int serverSelectorThreads = 3;
    // send oneway消息请求的并发 度(Broker端参数)
    private int serverOnewaySemaphoreValue = 256;
    // 异步消息发送的最大并发度 (Broker端参数)
    private int serverAsyncSemaphoreValue = 64;
    // 网络连接最大空闲时 间，默认为120s。如果连接空闲时间超过该参数设置的值，连接将被 关闭。
    private int serverChannelMaxIdleTimeSeconds = 120;

    // 网络socket发送缓存区大小，默认 为64KB
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    // 网络socket接收缓存区大小，默认 为64KB。
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    // ByteBuffer是否开 启缓存，建议开启。
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     *
     * 是否启用Epoll I/O模型，Linux 环境下建议开启。
     *
     * make make install
     *
     *
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    private boolean useEpollNativeSelector = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }
}
