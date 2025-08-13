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
package com.alibaba.dubbo.rpc.cluster;

import com.alibaba.dubbo.common.URL;

/**
 * 配置规则接口。(SPI, Prototype, ThreadSafe)
 * <p>
 * 配置规则用于在服务运行期间动态地调整服务的配置信息，主要功能包括：
 * <ul>
 * <li>动态调整服务权重</li>
 * <li>动态调整服务负载均衡策略</li>
 * <li>动态调整服务超时时间</li>
 * <li>动态调整服务重试次数</li>
 * </ul>
 * <p>
 * 配置规则可以按优先级排序，优先级高的配置规则优先生效。
 * 配置规则的来源可以是：
 * <ul>
 * <li>注册中心配置</li>
 * <li>动态配置中心</li>
 * <li>本地配置文件</li>
 * </ul>
 */
public interface Configurator extends Comparable<Configurator> {

    /**
     * 获取配置规则的URL。
     * <p>
     * URL中包含了配置规则的各种属性，如：
     * <ul>
     * <li>优先级</li>
     * <li>生效条件</li>
     * <li>配置项</li>
     * </ul>
     *
     * @return 配置规则的URL
     */
    URL getUrl();

    /**
     * 配置服务提供者的URL。
     * <p>
     * 根据配置规则修改服务提供者的URL，实现动态配置。
     * 配置规则可以修改URL中的任意参数，常见的配置项包括：
     * <ul>
     * <li>weight - 服务权重</li>
     * <li>loadbalance - 负载均衡策略</li>
     * <li>timeout - 调用超时时间</li>
     * <li>retries - 重试次数</li>
     * </ul>
     *
     * @param url 原服务提供者的URL
     * @return 配置后的服务提供者URL
     */
    URL configure(URL url);

}
