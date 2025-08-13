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
package com.alibaba.dubbo.remoting;

import com.alibaba.dubbo.common.URL;

import java.net.InetSocketAddress;

/**
 * 远程通信端点接口。(API/SPI, Prototype, ThreadSafe)
 * 
 * 该接口定义了远程通信的基本能力：
 * 1. 消息发送和关闭操作
 * 2. 地址和URL访问
 * 3. 通道处理器管理
 * 4. 生命周期控制
 * 
 * 主要特性：
 * - 线程安全：可以在多线程环境下使用
 * - 原型模式：每个实例独立工作
 * - 支持同步和异步操作
 * - 支持优雅关闭
 * 
 * @see com.alibaba.dubbo.remoting.Channel
 * @see com.alibaba.dubbo.remoting.Client
 * @see com.alibaba.dubbo.remoting.Server
 */
public interface Endpoint {

    /**
     * 获取端点的URL。
     * 
     * URL包含了端点的配置信息，如：
     * - 协议类型
     * - 地址和端口
     * - 超时时间
     * - 其他配置参数
     *
     * @return 包含端点配置的URL对象
     */
    URL getUrl();

    /**
     * 获取通道处理器。
     * 
     * 通道处理器负责处理：
     * - 连接的建立和断开
     * - 消息的接收和处理
     * - 异常情况的处理
     *
     * @return 负责处理通道事件的ChannelHandler对象
     */
    ChannelHandler getChannelHandler();

    /**
     * 获取本地地址。
     * 
     * 本地地址包含：
     * - IP地址
     * - 端口号
     * 
     * 对于服务器端，这是监听地址
     * 对于客户端，这是本地绑定地址
     *
     * @return 本地套接字地址
     */
    InetSocketAddress getLocalAddress();

    /**
     * 发送消息。
     * 
     * 此方法会同步发送消息，直到：
     * - 消息发送成功
     * - 发生异常
     * - 连接断开
     *
     * @param message 要发送的消息对象
     * @throws RemotingException 当发送失败或连接断开时抛出
     */
    void send(Object message) throws RemotingException;

    /**
     * 发送消息，并指定是否已经发送到socket。
     * 
     * 此方法提供了更细粒度的消息发送控制：
     * - 可以指定消息是否已经写入socket
     * - 适用于需要批量发送或特殊控制的场景
     *
     * @param message 要发送的消息对象
     * @param sent 消息是否已经发送到socket
     * @throws RemotingException 当发送失败或连接断开时抛出
     */
    void send(Object message, boolean sent) throws RemotingException;

    /**
     * 关闭通道。
     * 
     * 此方法会立即关闭通道：
     * - 停止接收新的请求
     * - 关闭底层连接
     * - 释放相关资源
     */
    void close();

    /**
     * 优雅地关闭通道。
     * 
     * 此方法会在指定的超时时间内完成关闭：
     * - 停止接收新的请求
     * - 等待当前请求处理完成
     * - 完成必要的清理工作
     * - 关闭底层连接
     * 
     * @param timeout 等待关闭完成的超时时间（毫秒）
     */
    void close(int timeout);

    /**
     * 开始关闭通道。
     * 
     * 此方法启动通道的关闭过程：
     * - 标记通道为正在关闭状态
     * - 停止接收新的请求
     * - 但不会立即关闭连接
     */
    void startClose();

    /**
     * 检查通道是否已关闭。
     * 
     * 通道在以下情况被视为已关闭：
     * - 显式调用close()后
     * - 发生致命错误后
     * - 远程端关闭连接后
     *
     * @return 如果通道已关闭返回true，否则返回false
     */
    boolean isClosed();

}