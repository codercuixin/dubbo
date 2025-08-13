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
package com.alibaba.dubbo.rpc;

import com.alibaba.dubbo.common.Node;

/**
 * Dubbo中的核心抽象，表示一个可执行的服务实例。(API/SPI, Prototype, ThreadSafe)
 * 
 * Invoker在不同场景下的角色：
 * 1. 服务提供者端：
 *    - 包装服务实现类
 *    - 处理RPC请求
 *    - 执行本地方法调用
 * 2. 服务消费者端：
 *    - 作为远程服务的代理
 *    - 发送RPC请求
 *    - 处理远程调用结果
 * 
 * 生命周期：
 * 1. 创建时机：
 *    - 提供者：服务导出时由Protocol实现创建
 *    - 消费者：服务引用时由Protocol实现创建
 * 2. 销毁时机：
 *    - 提供者：服务取消导出时
 *    - 消费者：服务取消引用时
 * 
 * 主要特性：
 * - 线程安全：支持并发请求处理
 * - 可扩展的：支持自定义实现
 * - 异常处理：统一的异常处理机制
 * - 泛化调用：支持泛化服务调用
 * 
 * @param <T> 服务接口类型
 * @see com.alibaba.dubbo.rpc.Protocol#refer(Class, com.alibaba.dubbo.common.URL)
 * @see com.alibaba.dubbo.rpc.InvokerListener
 * @see com.alibaba.dubbo.rpc.protocol.AbstractInvoker
 */
public interface Invoker<T> extends Node {

    /**
     * 获取服务接口类型。
     * 
     * 此方法返回当前Invoker所处理的实际服务接口类。
     * 例如：
     * - 如果是UserService的Invoker，返回UserService.class
     * - 如果是OrderService的Invoker，返回OrderService.class
     * 
     * 使用场景：
     * 1. 服务注册时获取接口信息
     * 2. 服务代理创建时获取接口类型
     * 3. 服务调用时进行类型检查
     *
     * @return 服务接口类
     */
    Class<T> getInterface();

    /**
     * 执行RPC调用。
     * 
     * 这是处理RPC调用的核心方法，其行为取决于Invoker的角色：
     * 1. 服务提供者端：
     *    - 执行实际的服务方法
     *    - 处理本地业务逻辑
     *    - 返回执行结果
     * 2. 服务消费者端：
     *    - 发送请求到远程提供者
     *    - 等待响应
     *    - 处理返回结果
     * 
     * 实现需要处理：
     * 1. 参数验证：
     *    - 检查参数类型
     *    - 验证参数值
     * 2. 方法调用：
     *    - 本地方法执行
     *    - 远程请求发送
     * 3. 异常处理：
     *    - 业务异常转换
     *    - 网络异常处理
     * 4. 结果处理：
     *    - 序列化/反序列化
     *    - 类型转换
     *
     * @param invocation 包含要调用的方法名、参数类型和参数值
     * @return 调用结果，包含实际的返回值或异常
     * @throws RpcException 当调用过程中发生错误时抛出，如网络错误
     */
    Result invoke(Invocation invocation) throws RpcException;

}