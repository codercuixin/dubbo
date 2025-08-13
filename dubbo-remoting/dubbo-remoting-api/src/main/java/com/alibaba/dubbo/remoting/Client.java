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

/**
 * 远程通信客户端接口。(API/SPI, Prototype, ThreadSafe)
 * 
 * 该接口继承了Endpoint、Channel和Resetable接口，提供：
 * 1. 基本的端点功能（来自Endpoint）
 * 2. 通道的数据传输能力（来自Channel）
 * 3. 重置功能（来自Resetable）
 * 4. 重连机制
 * 
 * 主要特性：
 * - 线程安全：支持多线程并发访问
 * - 原型模式：每个实例代表一个独立的客户端连接
 * - 自动重连：网络异常时可以自动重新建立连接
 * - 支持重置：可以重置客户端状态和配置
 * 
 * 更多信息参考：
 * <a href="http://en.wikipedia.org/wiki/Client%E2%80%93server_model">客户端/服务器模型</a>
 *
 * @see com.alibaba.dubbo.remoting.Transporter#connect(com.alibaba.dubbo.common.URL, ChannelHandler)
 */
public interface Client extends Endpoint, Channel, Resetable {

    /**
     * 重新建立连接。
     * 
     * 此方法用于：
     * - 连接断开后的手动重连
     * - 切换到新的服务器地址
     * - 解决网络异常问题
     * 
     * 重连过程：
     * 1. 关闭现有连接（如果有）
     * 2. 清理相关资源
     * 3. 创建新的连接
     * 4. 重新初始化客户端状态
     *
     * @throws RemotingException 如果重连失败
     */
    void reconnect() throws RemotingException;

    /**
     * 重置客户端参数。
     * 
     * @deprecated 此方法已废弃，请使用{@link Resetable#reset(URL)}代替
     * 
     * @param parameters 要重置的参数
     */
    @Deprecated
    void reset(com.alibaba.dubbo.common.Parameters parameters);

}