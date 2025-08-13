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
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;

/**
 * 注册中心工厂接口。(SPI, Singleton, ThreadSafe)
 * 
 * 该接口负责创建和管理注册中心实例：
 * 1. 实例创建：
 *    - 按需创建注册中心实例
 *    - 支持多种注册中心实现
 *    - 确保单例模式
 * 2. 实例管理：
 *    - 缓存已创建的实例
 *    - 管理实例生命周期
 *    - 处理实例销毁
 * 
 * 主要特性：
 * - 可扩展的：支持SPI机制
 * - 线程安全：支持并发访问
 * - 单例管理：相同配置共享实例
 * - 自适应：根据URL自动选择实现
 * 
 * 支持的实现：
 * - ZooKeeper注册中心
 * - Redis注册中心
 * - Multicast注册中心
 * - Nacos注册中心
 * 
 * 配置规则：
 * 1. 注册中心类型：protocol参数
 * 2. 地址信息：address参数
 * 3. 认证信息：username/password参数
 * 4. 其他配置：timeout、session等参数
 * 
 * @see com.alibaba.dubbo.registry.support.AbstractRegistryFactory
 */
@SPI("dubbo")
public interface RegistryFactory {

    /**
     * 连接注册中心。
     * 
     * 连接注册中心需要遵循以下约定：
     * 1. 连接检查：
     *    - URL设置check=false时，连接失败不抛异常
     *    - 否则连接失败时抛出异常
     * 
     * 2. 认证支持：
     *    - 支持URL中的username:password认证
     *    - 支持自定义认证机制
     * 
     * 3. 集群支持：
     *    - 支持backup=10.20.153.10备选地址
     *    - 支持故障自动切换
     * 
     * 4. 本地缓存：
     *    - 支持file=registry.cache本地磁盘缓存
     *    - 实现断网时的服务降级
     * 
     * 5. 超时配置：
     *    - 支持timeout=1000请求超时设置
     *    - 避免无限期等待
     * 
     * 6. 会话管理：
     *    - 支持session=60000会话超时设置
     *    - 自动处理会话过期
     * 
     * 配置示例：
     * zookeeper://10.20.153.10:2181?backup=10.20.153.11:2181,10.20.153.12:2181
     * &check=false&file=registry.cache&timeout=1000&session=60000
     *
     * @param url 注册中心地址，不允许为空
     * @return 注册中心引用，永远不会返回空值
     */
    @Adaptive({"protocol"})
    Registry getRegistry(URL url);

}