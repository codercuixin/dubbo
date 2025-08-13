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

import java.net.InetSocketAddress;

/**
 * 通道接口，表示一个网络连接。(API/SPI, Prototype, ThreadSafe)
 * 
 * 该接口扩展了Endpoint接口，提供了以下额外功能：
 * 1. 远程地址访问
 * 2. 连接状态检查
 * 3. 属性的存取
 * 
 * 主要特性：
 * - 线程安全：支持多线程并发访问
 * - 原型模式：每个实例代表一个独立的连接
 * - 支持属性扩展：可以存储自定义属性
 * - 连接状态管理：可以检查连接是否活跃
 * 
 * 通道可以是：
 * - 客户端连接（Client）
 * - 服务器端接受的连接（Server.getChannels()）
 * 
 * @see com.alibaba.dubbo.remoting.Client
 * @see com.alibaba.dubbo.remoting.Server#getChannels()
 * @see com.alibaba.dubbo.remoting.Server#getChannel(InetSocketAddress)
 */
public interface Channel extends Endpoint {

    /**
     * 获取远程地址。
     * 
     * 远程地址包含：
     * - 对端的IP地址
     * - 对端的端口号
     * 
     * 对于服务器端，这是客户端的地址
     * 对于客户端，这是服务器的地址
     *
     * @return 远程端的套接字地址
     */
    InetSocketAddress getRemoteAddress();

    /**
     * 检查通道是否处于连接状态。
     * 
     * 通道在以下情况被视为已连接：
     * - 底层socket连接正常
     * - 可以收发消息
     * - 未被标记为关闭
     *
     * @return 如果通道处于连接状态返回true，否则返回false
     */
    boolean isConnected();

    /**
     * 检查是否存在指定的属性。
     * 
     * 属性是与通道关联的键值对，可用于：
     * - 存储会话信息
     * - 保存统计数据
     * - 附加自定义数据
     *
     * @param key 属性的键名
     * @return 如果属性存在返回true，否则返回false
     */
    boolean hasAttribute(String key);

    /**
     * 获取指定属性的值。
     * 
     * 如果属性不存在，返回null。
     * 属性值可以是任意类型的对象。
     *
     * @param key 属性的键名
     * @return 属性值，如果属性不存在则返回null
     */
    Object getAttribute(String key);

    /**
     * 设置属性值。
     * 
     * 如果属性已存在，会覆盖原有的值。
     * 如果值为null，相当于删除该属性。
     *
     * @param key 属性的键名
     * @param value 属性的值
     */
    void setAttribute(String key, Object value);

    /**
     * 移除指定的属性。
     * 
     * 如果属性不存在，此操作不会产生任何效果。
     * 移除后通过hasAttribute()和getAttribute()
     * 将无法再访问该属性。
     *
     * @param key 要移除的属性的键名
     */
    void removeAttribute(String key);

}