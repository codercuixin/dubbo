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

import com.alibaba.dubbo.common.extension.SPI;


/**
 * 通道事件处理器接口。(API, Prototype, ThreadSafe)
 * 
 * 该接口定义了处理通道生命周期事件和消息的方法：
 * 1. 连接事件：connected, disconnected
 * 2. 消息事件：sent, received
 * 3. 异常处理：caught
 * 
 * 主要特性：
 * - 线程安全：支持多线程并发调用
 * - 原型模式：每个通道可以有独立的处理器实例
 * - 可扩展的：支持SPI扩展机制
 * - 异步处理：所有方法都支持异步调用
 * 
 * 使用场景：
 * 1. 服务器端处理客户端连接
 * 2. 客户端处理与服务器的交互
 * 3. 实现自定义的消息处理逻辑
 * 4. 处理网络异常和错误
 *
 * @see com.alibaba.dubbo.remoting.Transporter#bind(com.alibaba.dubbo.common.URL, ChannelHandler)
 * @see com.alibaba.dubbo.remoting.Transporter#connect(com.alibaba.dubbo.common.URL, ChannelHandler)
 */
@SPI
public interface ChannelHandler {

    /**
     * 当通道连接建立时调用。
     * 
     * 此方法在以下情况下被调用：
     * - 客户端成功连接到服务器时
     * - 服务器接受新的客户端连接时
     * 
     * 实现可以在此方法中：
     * - 初始化连接相关的资源
     * - 发送初始化消息
     * - 记录连接信息
     *
     * @param channel 新建立的通道
     * @throws RemotingException 如果处理连接事件时发生错误
     */
    void connected(Channel channel) throws RemotingException;

    /**
     * 当通道连接断开时调用。
     * 
     * 此方法在以下情况下被调用：
     * - 客户端主动断开连接时
     * - 服务器关闭连接时
     * - 网络异常导致连接断开时
     * - 心跳超时断开时
     * 
     * 实现可以在此方法中：
     * - 清理连接相关的资源
     * - 记录断开原因
     * - 触发重连机制
     *
     * @param channel 已断开的通道
     * @throws RemotingException 如果处理断开事件时发生错误
     */
    void disconnected(Channel channel) throws RemotingException;

    /**
     * 当消息发送完成时调用。
     * 
     * 此方法在消息被写入通道后调用，可用于：
     * - 确认消息发送状态
     * - 更新统计信息
     * - 触发后续操作
     * - 释放相关资源
     * 
     * 注意：此方法不保证消息已被对方接收，
     * 只是表示消息已经写入网络缓冲区。
     *
     * @param channel 发送消息的通道
     * @param message 已发送的消息对象
     * @throws RemotingException 如果处理发送完成事件时发生错误
     */
    void sent(Channel channel, Object message) throws RemotingException;

    /**
     * 当收到消息时调用。
     * 
     * 此方法在接收到远程节点的消息时调用，用于：
     * - 处理接收到的消息
     * - 执行业务逻辑
     * - 返回响应
     * - 更新统计信息
     * 
     * 实现注意事项：
     * - 消息处理应该是非阻塞的
     * - 复杂的业务逻辑应该放在单独的线程中执行
     * - 注意处理各种消息类型
     *
     * @param channel 接收消息的通道
     * @param message 接收到的消息对象
     * @throws RemotingException 如果处理接收消息时发生错误
     */
    void received(Channel channel, Object message) throws RemotingException;

    /**
     * 当捕获到异常时调用。
     * 
     * 此方法用于处理通道操作中的异常，包括：
     * - 网络通信异常
     * - 消息编解码异常
     * - 消息处理异常
     * - 其他运行时异常
     * 
     * 实现应该：
     * - 记录异常信息
     * - 进行必要的错误恢复
     * - 决定是否关闭通道
     * - 通知相关方异常发生
     *
     * @param channel 发生异常的通道
     * @param exception 捕获到的异常对象
     * @throws RemotingException 如果处理异常时发生错误
     */
    void caught(Channel channel, Throwable exception) throws RemotingException;

}