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

import com.alibaba.dubbo.common.Resetable;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * 远程通信服务器接口。(API/SPI, Prototype, ThreadSafe)
 * 
 * 该接口继承了Endpoint和Resetable接口，提供：
 * 1. 基本的端点功能（来自Endpoint）
 * 2. 重置功能（来自Resetable）
 * 3. 客户端连接管理
 * 4. 绑定状态查询
 * 
 * 主要特性：
 * - 线程安全：支持多线程并发访问
 * - 原型模式：每个实例代表一个独立的服务器
 * - 连接管理：维护所有客户端连接
 * - 支持重置：可以重置服务器状态和配置
 * 
 * 服务器职责：
 * 1. 监听端口，接受客户端连接
 * 2. 管理所有客户端连接
 * 3. 处理客户端请求
 * 4. 维护服务器状态
 * 
 * 更多信息参考：
 * <a href="http://en.wikipedia.org/wiki/Client%E2%80%93server_model">客户端/服务器模型</a>
 *
 * @see com.alibaba.dubbo.remoting.Transporter#bind(com.alibaba.dubbo.common.URL, ChannelHandler)
 */
public interface Server extends Endpoint, Resetable {

    /**
     * 检查服务器是否已绑定到端口。
     * 
     * 服务器在以下情况被视为已绑定：
     * - 成功监听指定端口
     * - 可以接受新的连接
     * - 未被关闭
     *
     * @return 如果服务器已绑定返回true，否则返回false
     */
    boolean isBound();

    /**
     * 获取所有当前连接的客户端通道。
     * 
     * 此方法返回的集合：
     * - 包含所有活跃的客户端连接
     * - 是当前时刻的快照
     * - 可能随时发生变化
     * - 不保证线程安全
     *
     * @return 当前所有客户端通道的集合
     */
    Collection<Channel> getChannels();

    /**
     * 根据远程地址获取对应的客户端通道。
     * 
     * 此方法用于：
     * - 查找特定客户端的连接
     * - 向指定客户端发送消息
     * - 管理特定客户端连接
     *
     * @param remoteAddress 客户端的远程地址
     * @return 对应的客户端通道，如果不存在则返回null
     */
    Channel getChannel(InetSocketAddress remoteAddress);

    /**
     * 重置服务器参数。
     * 
     * @deprecated 此方法已废弃，请使用{@link Resetable#reset(URL)}代替
     * 
     * @param parameters 要重置的参数
     */
    @Deprecated
    void reset(com.alibaba.dubbo.common.Parameters parameters);

}