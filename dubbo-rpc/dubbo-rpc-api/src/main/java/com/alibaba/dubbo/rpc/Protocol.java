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

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;

/**
 * RPC通信协议接口。(API/SPI, Singleton, ThreadSafe)
 * 
 * 该接口定义了Dubbo中所有RPC协议的核心契约：
 * 1. 服务导出 - 使服务可被远程调用
 * 2. 服务引用 - 创建远程服务的代理
 * 3. 协议生命周期管理
 * 
 * 主要特性：
 * - 线程安全：所有实现必须支持并发访问
 * - 单例模式：每种协议类型使用单例
 * - 可扩展的：支持SPI机制动态加载协议实现
 * - 幂等性：服务导出操作必须是幂等的
 * 
 * 支持的协议类型：
 * - dubbo: 默认协议
 * - http: HTTP协议
 * - hessian: Hessian协议
 * - rmi: Java RMI协议
 * - webservice: WebService协议
 * 
 * 协议职责：
 * 1. 维护协议的端口和连接
 * 2. 管理服务的导出和引用
 * 3. 处理请求和响应的编解码
 * 4. 支持协议的优雅停机
 * 
 * @see com.alibaba.dubbo.rpc.Invoker
 * @see com.alibaba.dubbo.rpc.Exporter
 */
@SPI("dubbo")
public interface Protocol {

    /**
     * 获取协议的默认端口。
     * 
     * 当用户未配置端口时，使用此端口：
     * - dubbo协议默认端口：20880
     * - rmi协议默认端口：1099
     * - http协议默认端口：80
     * - hessian协议默认端口：8080
     * - webservice协议默认端口：8080
     *
     * @return 协议的默认端口号
     */
    int getDefaultPort();

    /**
     * 导出服务供远程调用。
     * 
     * 实现要求：
     * 1. 记录请求来源地址：
     *    RpcContext.getContext().setRemoteAddress();
     * 2. 必须是幂等的：
     *    - 对相同URL的多次导出效果相同
     *    - 避免重复导出和端口冲突
     * 3. 框架负责传入Invoker实例：
     *    - 协议实现无需关心Invoker的创建
     *    - 专注于网络传输和协议编解码
     * 
     * 导出过程：
     * 1. 启动协议服务器（如果未启动）
     * 2. 注册服务到注册中心
     * 3. 创建服务导出器
     * 4. 初始化相关资源
     * 
     * @param <T> 服务接口类型
     * @param invoker 服务的执行体，包含服务的具体实现
     * @return 服务导出器，用于后续取消导出服务
     * @throws RpcException 当服务导出失败时抛出，如：端口被占用
     */
    @Adaptive
    <T> Exporter<T> export(Invoker<T> invoker) throws RpcException;

    /**
     * 引用远程服务。
     * 
     * 实现要求：
     * 1. 当用户调用返回的Invoker对象的invoke()方法时：
     *    - 协议需要正确执行远程调用
     *    - 处理调用结果和异常
     * 2. 协议负责实现返回的Invoker对象：
     *    - 通常在Invoker实现中发送远程请求
     *    - 处理请求的序列化和反序列化
     * 3. 容错处理：
     *    - 当URL中设置check=false时
     *    - 连接失败时不抛出异常
     *    - 尝试进行错误恢复
     * 
     * 引用过程：
     * 1. 创建客户端连接
     * 2. 生成代理对象
     * 3. 初始化远程调用相关资源
     * 4. 处理超时和重试配置
     *
     * @param <T> 服务接口类型
     * @param type 服务接口类
     * @param url 远程服务的URL地址
     * @return 服务的本地代理调用器
     * @throws RpcException 当连接服务提供者失败时抛出
     */
    @Adaptive
    <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException;

    /**
     * 销毁协议。
     * 
     * 销毁过程：
     * 1. 取消该协议的所有服务导出和引用：
     *    - 从注册中心注销服务
     *    - 关闭所有客户端连接
     * 2. 释放所有占用的资源：
     *    - 关闭网络连接
     *    - 释放端口
     *    - 清理缓存
     *    - 停止线程池
     * 
     * 特别说明：
     * - 协议销毁后仍可继续导出和引用新服务
     * - 这允许协议实现进行资源回收和重建
     * - 实现应保证优雅停机
     */
    void destroy();

}