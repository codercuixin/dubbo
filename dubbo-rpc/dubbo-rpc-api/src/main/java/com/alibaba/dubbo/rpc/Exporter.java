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

/**
 * 服务导出器接口。(API/SPI, Prototype, ThreadSafe)
 * 
 * 该接口负责管理服务的导出生命周期：
 * 1. 维护服务的导出状态
 * 2. 管理服务的Invoker对象
 * 3. 支持服务的取消导出
 * 
 * 主要特性：
 * - 线程安全：支持多线程并发访问
 * - 原型模式：每个导出服务有独立的实例
 * - 生命周期管理：控制服务的导出和取消导出
 * 
 * 使用场景：
 * 1. 服务提供者导出服务
 * 2. 服务注册中心管理服务
 * 3. 服务动态配置和管理
 * 
 * @param <T> 服务接口类型
 * @see com.alibaba.dubbo.rpc.Protocol#export(Invoker)
 * @see com.alibaba.dubbo.rpc.ExporterListener
 * @see com.alibaba.dubbo.rpc.protocol.AbstractExporter
 */
public interface Exporter<T> {

    /**
     * 获取服务的Invoker对象。
     * 
     * Invoker对象包含了服务的：
     * - 接口定义
     * - 具体实现
     * - 调用方式
     * - 配置信息
     *
     * @return 服务的Invoker对象
     */
    Invoker<T> getInvoker();

    /**
     * 取消服务的导出。
     * 
     * 此方法会：
     * 1. 从注册中心注销服务
     * 2. 停止接收新的请求
     * 3. 等待当前请求处理完成
     * 4. 销毁相关资源
     * 
     * 实现通常会调用：
     * <code>
     * getInvoker().destroy();
     * </code>
     * 
     * 注意：
     * - 此操作不可逆
     * - 调用后服务将不再可用
     * - 应确保所有请求处理完成
     */
    void unexport();

}