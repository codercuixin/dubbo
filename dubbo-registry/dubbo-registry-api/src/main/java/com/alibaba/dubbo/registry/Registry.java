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

import com.alibaba.dubbo.common.Node;

/**
 * 注册中心接口。(SPI, Prototype, ThreadSafe)
 * 
 * 该接口继承了Node和RegistryService：
 * 1. Node：提供基础的节点能力
 *    - 获取配置URL
 *    - 查看可用状态
 * 2. RegistryService：提供注册中心核心功能
 *    - 服务注册与注销
 *    - 服务订阅与取消订阅
 *    - 服务查询
 * 
 * 主要特性：
 * - 可扩展的：支持SPI机制
 * - 线程安全：支持并发访问
 * - 状态可查：支持健康检查
 * - 配置灵活：支持动态配置
 * 
 * 支持的实现：
 * - ZooKeeper注册中心
 * - Redis注册中心
 * - Multicast注册中心
 * - Nacos注册中心
 * 
 * 使用场景：
 * 1. 服务注册
 *    - 服务提供者注册
 *    - 服务元数据存储
 * 2. 服务发现
 *    - 服务消费者订阅
 *    - 服务列表获取
 * 3. 服务治理
 *    - 服务路由规则
 *    - 服务配置管理
 * 4. 服务监控
 *    - 服务状态监控
 *    - 服务质量追踪
 * 
 * @see com.alibaba.dubbo.registry.RegistryFactory#getRegistry(com.alibaba.dubbo.common.URL)
 * @see com.alibaba.dubbo.registry.support.AbstractRegistry
 */
public interface Registry extends Node, RegistryService {
}