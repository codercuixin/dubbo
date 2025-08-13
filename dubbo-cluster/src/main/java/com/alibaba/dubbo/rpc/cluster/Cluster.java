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

import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.support.FailoverCluster;

/**
 * 集群接口。(SPI, Singleton, ThreadSafe)
 * <p>
 * 集群接口用于将多个服务提供者合并为一个虚拟的服务提供者，并提供以下功能：
 * <ul>
 * <li>故障转移：当调用失败时，自动切换到其他可用的服务提供者</li>
 * <li>负载均衡：在多个服务提供者之间分配请求</li>
 * <li>服务目录：管理服务提供者列表</li>
 * </ul>
 * <p>
 * 相关链接：
 * <a href="http://en.wikipedia.org/wiki/Computer_cluster">集群</a>
 * <a href="http://en.wikipedia.org/wiki/Fault-tolerant_system">容错系统</a>
 *
 * @see com.alibaba.dubbo.rpc.cluster.support.FailoverCluster
 */
@SPI(FailoverCluster.NAME)
public interface Cluster {

    /**
     * 将目录中的多个服务提供者合并为一个虚拟的服务提供者。
     * 
     * @param <T> 服务的泛型类型
     * @param directory 服务目录，包含了所有可用的服务提供者列表
     * @return 集群服务提供者，封装了容错、负载均衡等集群能力
     * @throws RpcException 当合并过程中发生错误时抛出此异常
     */
    @Adaptive
    <T> Invoker<T> join(Directory<T> directory) throws RpcException;

}