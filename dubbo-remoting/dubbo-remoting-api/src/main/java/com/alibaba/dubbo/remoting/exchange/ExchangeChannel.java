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

import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;

/**
 * 支持请求-响应式通信的扩展通道接口。(API/SPI, Prototype, ThreadSafe)
 * 
 * 该接口扩展了基本的Channel接口，提供以下功能：
 * 1. 通过request()方法支持请求-响应模式
 * 2. 通过ResponseFuture支持异步响应处理
 * 3. 提供交换消息处理能力
 * 4. 支持优雅关闭
 * 
 * ExchangeChannel是Dubbo中RPC通信的基础，
 * 支持在网络连接上进行同步和异步的请求-响应模式通信。
 * 
 * @see com.alibaba.dubbo.remoting.Channel
 * @see ResponseFuture
 * @see ExchangeHandler
 */
public interface ExchangeChannel extends Channel {

    /**
     * 发送请求消息并返回用于获取响应的Future对象。
     * 
     * 此方法实现了异步的请求-响应模式：
     * 1. 向远程端点发送请求消息
     * 2. 立即返回一个ResponseFuture对象
     * 3. 响应可以稍后通过ResponseFuture获取
     * 
     * 使用通道配置的默认超时值。
     *
     * @param request 要发送的请求消息
     * @return 一个ResponseFuture对象，当响应到达时可从中获取响应
     * @throws RemotingException 如果请求无法发送
     */
    ResponseFuture request(Object request) throws RemotingException;

    /**
     * 发送请求消息并指定超时时间，返回用于获取响应的Future对象。
     * 
     * 类似于request(Object)，但允许指定自定义超时：
     * 1. 向远程端点发送请求消息
     * 2. 立即返回一个ResponseFuture对象
     * 3. 响应必须在指定的超时时间内到达
     * 4. 如果发生超时，Future将以异常完成
     *
     * @param request 要发送的请求消息
     * @param timeout 等待响应的最大时间（毫秒）
     * @return 一个ResponseFuture对象，当响应到达时可从中获取响应
     * @throws RemotingException 如果请求无法发送
     */
    ResponseFuture request(Object request, int timeout) throws RemotingException;

    /**
     * 获取与此通道关联的交换处理器。
     * 
     * 交换处理器负责处理此通道上的传入请求
     * 和响应。它包含了处理不同类型消息的业务逻辑。
     *
     * @return 处理此通道消息的ExchangeHandler
     */
    ExchangeHandler getExchangeHandler();

    /**
     * 使用指定的超时时间优雅地关闭通道。
     * 
     * 优雅关闭确保：
     * 1. 不再接受新的请求
     * 2. 允许待处理的请求完成
     * 3. 正确释放资源
     * 4. 清理完成后关闭底层连接
     *
     * @param timeout 等待待处理请求的最大时间（毫秒）
     */
    @Override
    void close(int timeout);

}
