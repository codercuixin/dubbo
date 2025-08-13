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
package com.alibaba.dubbo.remoting.exchange;

import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.telnet.TelnetHandler;

/**
 * 用于处理请求-响应式消息的处理器。(API, Prototype, ThreadSafe)
 * 
 * 该接口结合了ChannelHandler和TelnetHandler的功能：
 * 1. 基本的通道事件处理（连接、断开、发送、接收）
 * 2. 用于远程管理的Telnet命令处理
 * 3. 请求-响应消息处理
 * 
 * ExchangeHandler是Dubbo远程通信层的关键组件：
 * - 处理业务请求并返回响应
 * - 处理连接生命周期事件
 * - 支持通过telnet进行管理
 * 
 * 实现必须是线程安全的，因为它们会被并发调用。
 * 
 * @see com.alibaba.dubbo.remoting.ChannelHandler
 * @see com.alibaba.dubbo.remoting.telnet.TelnetHandler
 * @see ExchangeChannel
 */
public interface ExchangeHandler extends ChannelHandler, TelnetHandler {

    /**
     * 处理请求并返回响应。
     * 
     * 此方法是请求-响应处理的核心：
     * 1. 接收来自远程对等方的请求
     * 2. 根据业务逻辑处理请求
     * 3. 返回将发送回请求方的响应
     * 
     * 实现应处理以下方面：
     * - 参数验证
     * - 请求反序列化
     * - 业务逻辑执行
     * - 响应序列化
     * - 错误处理
     *
     * @param channel 接收请求的交换通道
     * @param request 要处理的请求对象
     * @return 将发送回请求方的响应对象
     * @throws RemotingException 如果在请求处理过程中发生任何错误
     */
    Object reply(ExchangeChannel channel, Object request) throws RemotingException;

}