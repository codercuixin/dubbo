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
 * 注册中心服务接口。(SPI, Prototype, ThreadSafe)
 * 
 * 该接口定义了注册中心的核心服务能力：
 * 1. 服务注册与注销
 *    - 提供者注册
 *    - 消费者注册
 *    - 路由规则注册
 *    - 配置信息注册
 * 2. 服务订阅与取消订阅
 *    - 服务变更通知
 *    - 配置变更通知
 *    - 路由规则变更通知
 * 3. 服务查询
 *    - 提供者列表查询
 *    - 消费者列表查询
 *    - 路由规则查询
 * 
 * 主要特性：
 * - 可扩展的：支持多种实现
 * - 异步通知：服务变更推送
 * - 数据分类：支持数据分组
 * - 可持久化：支持数据持久化
 * 
 * 数据存储：
 * 1. 临时数据
 *    - 动态服务数据
 *    - 运行时状态
 * 2. 持久数据
 *    - 静态配置数据
 *    - 路由规则数据
 * 
 * 通知机制：
 * 1. 推模式
 *    - 数据变更实时通知
 *    - 支持批量通知
 * 2. 拉模式
 *    - 定时轮询查询
 *    - 按需主动查询
 * 
 * @see com.alibaba.dubbo.registry.Registry
 * @see com.alibaba.dubbo.registry.RegistryFactory#getRegistry(URL)
 */
public interface RegistryService {

    /**
     * 注册数据，如：提供者服务、消费者地址、路由规则、覆盖规则等。
     * 
     * 注册需要遵循以下约定：
     * 1. 容错处理：
     *    - URL设置check=false时，注册失败不抛异常
     *    - 失败后后台重试
     *    - 否则立即抛出异常
     * 
     * 2. 数据持久化：
     *    - URL设置dynamic=false时，数据需持久存储
     *    - 否则注册方异常退出时自动删除
     * 
     * 3. 数据分类：
     *    - URL设置category=routers时，表示分类存储
     *    - 默认分类为providers
     *    - 支持按分类通知数据
     * 
     * 4. 数据可靠性：
     *    - 注册中心重启时数据不丢失
     *    - 网络抖动时数据不丢失
     *    - 断线时自动删除数据
     * 
     * 5. 并存规则：
     *    - 允许URL相同但参数不同的数据并存
     *    - 不同参数的数据不能互相覆盖
     *
     * @param url 注册信息，不允许为空
     *           示例：dubbo://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     */
    void register(URL url);

    /**
     * 注销数据。
     * 
     * 注销需要遵循以下约定：
     * 1. 持久化数据处理：
     *    - 对于dynamic=false的持久化数据
     *    - 如果找不到注册数据则抛出IllegalStateException
     *    - 否则忽略错误继续处理
     * 
     * 2. 匹配规则：
     *    - 根据完整的URL进行匹配
     *    - 包括所有URL参数
     *    - 精确匹配不支持通配
     * 
     * 使用场景：
     * 1. 服务下线
     * 2. 服务迁移
     * 3. 临时维护
     * 4. 配置变更
     *
     * @param url 注册信息，不允许为空
     *           示例：dubbo://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     */
    void unregister(URL url);

    /**
     * 订阅符合条件的注册数据，当注册数据变更时自动推送。
     * 
     * 订阅需要遵循以下约定：
     * 1. 容错处理：
     *    - URL设置check=false时，订阅失败不抛异常
     *    - 失败后后台重试
     * 
     * 2. 数据分类订阅：
     *    - URL设置category=routers时，只通知指定分类数据
     *    - 多个分类用逗号分隔
     *    - 支持星号通配，表示订阅所有分类数据
     * 
     * 3. 条件过滤：
     *    - 支持接口、分组、版本、分类器作为条件
     *    - 示例：interface=com.alibaba.foo.BarService&version=1.0.0
     * 
     * 4. 条件匹配：
     *    - 支持星号通配
     *    - 可订阅所有接口的所有版本
     *    - 示例：interface=*&group=*&version=*&classifier=*
     * 
     * 5. 订阅恢复：
     *    - 注册中心重启时自动恢复订阅
     *    - 网络抖动时自动恢复订阅
     * 
     * 6. 并存规则：
     *    - 允许URL相同但参数不同的订阅并存
     *    - 不同参数的订阅不能互相覆盖
     * 
     * 7. 阻塞处理：
     *    - 订阅过程必须阻塞
     *    - 第一次通知完成后才返回
     *
     * @param url 订阅条件，不允许为空
     *           示例：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @param listener 变更事件监听器，不允许为空
     */
    void subscribe(URL url, NotifyListener listener);

    /**
     * 取消订阅。
     * 
     * 取消订阅需要遵循以下约定：
     * 1. 未订阅处理：
     *    - 如果没有订阅，直接忽略
     *    - 不抛出异常
     * 
     * 2. 匹配规则：
     *    - 根据完整的URL进行匹配
     *    - 包括所有URL参数
     *    - 必须与订阅时的URL完全一致
     * 
     * 使用场景：
     * 1. 服务下线
     * 2. 服务迁移
     * 3. 动态配置变更
     * 4. 临时取消订阅
     *
     * @param url 订阅条件，不允许为空
     *           示例：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @param listener 变更事件监听器，不允许为空
     */
    void unsubscribe(URL url, NotifyListener listener);

    /**
     * 查询符合条件的已注册数据。
     * 
     * 这是与订阅推模式对应的拉模式：
     * - 只返回一次结果
     * - 不会持续推送变更
     * - 支持条件过滤
     * 
     * 查询特性：
     * 1. 即时查询：
     *    - 返回当前最新数据
     *    - 不保证与推模式的一致性
     * 
     * 2. 条件过滤：
     *    - 支持接口、分组、版本过滤
     *    - 支持分类数据过滤
     *    - 支持自定义参数过滤
     * 
     * 3. 结果处理：
     *    - 返回URL列表可能为空
     *    - 返回数据格式与通知回调相同
     *    - 支持数据分组返回
     * 
     * 使用场景：
     * 1. 服务发现
     * 2. 配置获取
     * 3. 路由规则查询
     * 4. 服务状态检查
     *
     * @param url 查询条件，不允许为空
     *           示例：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @return 已注册的信息列表，可能为空，数据格式与{@link NotifyListener#notify(List)}的参数含义相同
     * @see com.alibaba.dubbo.registry.NotifyListener#notify(List)
     */
    List<URL> lookup(URL url);

}