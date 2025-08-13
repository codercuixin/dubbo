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

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.exchange.support.header.HeaderExchanger;

/**
 * 用于创建交换客户端和服务器的工厂接口。(SPI, Singleton, ThreadSafe)
 * 
 * Exchanger负责创建支持请求-响应消息交换模式的网络通信端点。
 * 它提供两个主要功能：
 * 1. 创建接受客户端连接的服务器
 * 2. 创建连接到服务器的客户端
 * 
 * 主要特性：
 * - 可通过SPI扩展：可以插入不同的实现
 * - 线程安全：可以在线程间安全共享
 * - 单例：每个实现只需要一个实例
 * - 支持同步和异步通信
 * 
 * 默认实现是HeaderExchanger，它为请求-响应关联
 * 添加了消息头。
 * 
 * @see <a href="http://en.wikipedia.org/wiki/Message_Exchange_Pattern">消息交换模式</a>
 * @see <a href="http://en.wikipedia.org/wiki/Request-response">请求-响应模式</a>
 * @see ExchangeClient
 * @see ExchangeServer
 * @see HeaderExchanger
 */
@SPI(HeaderExchanger.NAME)
public interface Exchanger {

    /**
     * 创建并绑定交换服务器到指定的URL。
     * 
     * 此方法：
     * 1. 创建一个新的交换服务器实例
     * 2. 将其绑定到URL指定的端口
     * 3. 使用提供的处理器配置它
     * 4. 开始接受客户端连接
     * 
     * 服务器配置通过URL参数指定。
     *
     * @param url 包含服务器配置参数的URL
     * @param handler 将处理传入请求的处理器
     * @return 一个准备好接受连接的新ExchangeServer实例
     * @throws RemotingException 如果服务器无法创建或绑定
     */
    @Adaptive({Constants.EXCHANGER_KEY})
    ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException;

    /**
     * 创建并连接交换客户端到远程服务器。
     * 
     * 此方法：
     * 1. 创建一个新的交换客户端实例
     * 2. 将其连接到URL指定的服务器
     * 3. 使用提供的处理器配置它
     * 4. 建立连接并在就绪时返回
     * 
     * 客户端配置通过URL参数指定。
     * 连接尝试将根据URL参数设置的超时时间超时。
     *
     * @param url 包含服务器地址和客户端配置的URL
     * @param handler 将处理服务器响应的处理器
     * @return 一个已连接且就绪的新ExchangeClient实例
     * @throws RemotingException 如果客户端无法连接到服务器
     */
    @Adaptive({Constants.EXCHANGER_KEY})
    ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException;

}