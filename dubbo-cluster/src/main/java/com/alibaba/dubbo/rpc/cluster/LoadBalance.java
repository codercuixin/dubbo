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
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.loadbalance.RandomLoadBalance;

import java.util.List;

/**
 * 负载均衡接口。(SPI, Singleton, ThreadSafe)
 * <p>
 * 负载均衡策略用于从多个服务提供者中选择一个进行调用。默认提供以下几种实现：
 * <ul>
 * <li>随机 - 随机选择一个服务提供者</li>
 * <li>轮询 - 依次轮流调用每个服务提供者</li>
 * <li>最少活跃调用 - 选择当前最少在处理请求的服务提供者</li>
 * <li>一致性哈希 - 相同参数的请求总是发到同一个服务提供者</li>
 * </ul>
 * <p>
 * 相关链接：
 * <a href="http://en.wikipedia.org/wiki/Load_balancing_(computing)">负载均衡</a>
 *
 * @see com.alibaba.dubbo.rpc.cluster.Cluster#join(Directory)
 * @see com.alibaba.dubbo.rpc.cluster.loadbalance.RandomLoadBalance
 */
@SPI(RandomLoadBalance.NAME)
public interface LoadBalance {

    /**
     * 从服务提供者列表中选择一个服务提供者。
     *
     * @param invokers   服务提供者列表
     * @param url        服务引用URL，包含了负载均衡配置信息
     * @param invocation 调用信息，可用于参与负载均衡决策（如一致性哈希）
     * @return 被选中的服务提供者
     * @throws RpcException 当无法选择到合适的服务提供者时抛出此异常
     */
    @Adaptive("loadbalance")
    <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException;

}