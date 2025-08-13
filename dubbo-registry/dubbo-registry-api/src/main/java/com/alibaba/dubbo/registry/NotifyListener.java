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
package com.alibaba.dubbo.registry;

import com.alibaba.dubbo.common.URL;

import java.util.List;

/**
 * 注册数据变更通知监听器。(API, Prototype, ThreadSafe)
 * 
 * 该接口用于处理注册中心的数据变更通知：
 * 1. 服务变更通知
 *    - 提供者列表变更
 *    - 消费者列表变更
 *    - 路由规则变更
 * 2. 配置变更通知
 *    - 动态配置变更
 *    - 服务降级配置
 *    - 权重调整配置
 * 
 * 主要特性：
 * - 线程安全：支持并发通知
 * - 有序处理：保证通知顺序
 * - 分类通知：支持分类数据
 * - 全量通知：不支持增量通知
 * 
 * 使用场景：
 * 1. 服务发现
 * 2. 配置变更
 * 3. 服务治理
 * 4. 动态路由
 * 
 * @see com.alibaba.dubbo.registry.RegistryService#subscribe(URL, NotifyListener)
 */
public interface NotifyListener {

    /**
     * 当接收到服务变更通知时触发。
     * 
     * 通知需要遵循以下约定：
     * 1. 通知粒度：
     *    - 按服务接口和数据类型维度通知
     *    - 同一服务的同类数据必须全量通知
     *    - 不需要比较前次通知结果
     * 
     * 2. 首次通知：
     *    - 必须是服务的全量数据
     *    - 包含所有数据类型
     *    - 即使数据为空也要通知
     * 
     * 3. 变更通知：
     *    - 允许分类型通知：providers、consumers等
     *    - 每种类型数据必须是全量的
     *    - 不支持增量数据通知
     * 
     * 4. 空数据处理：
     *    - 需要通知空协议
     *    - 使用category参数标识数据类型
     *    - URL中包含空协议标识
     * 
     * 5. 通知顺序：
     *    - 注册中心实现必须保证顺序
     *    - 支持的方式：
     *      - 单线程推送
     *      - 队列序列化
     *      - 版本号比对
     * 
     * 通知分类：
     * 1. 服务提供者：providers
     * 2. 服务消费者：consumers
     * 3. 路由规则：routers
     * 4. 配置信息：overrides
     *
     * @param urls 已注册的信息列表，永远不会为空，
     *            数据格式与{@link RegistryService#lookup(URL)}的返回值相同
     */
    void notify(List<URL> urls);

}