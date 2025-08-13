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

import com.alibaba.dubbo.common.Node;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;

import java.util.List;

/**
 * 服务目录接口。(SPI, Prototype, ThreadSafe)
 * <p>
 * 服务目录用于维护服务提供者列表，它的职责包括：
 * <ul>
 * <li>动态维护服务提供者列表</li>
 * <li>根据路由规则和配置规则过滤服务提供者</li>
 * <li>监听注册中心变更并更新服务提供者列表</li>
 * </ul>
 * <p>
 * 服务目录和注册中心的关系：
 * <ul>
 * <li>注册中心负责服务的注册与发现</li>
 * <li>服务目录负责服务的路由与过滤</li>
 * <li>服务目录从注册中心获取服务提供者列表</li>
 * </ul>
 * <p>
 * 相关链接：
 * <a href="http://en.wikipedia.org/wiki/Directory_service">目录服务</a>
 *
 * @see com.alibaba.dubbo.rpc.cluster.Cluster#join(Directory)
 */
public interface Directory<T> extends Node {

    /**
     * 获取服务接口类型。
     *
     * @return 服务接口类型
     */
    Class<T> getInterface();

    /**
     * 列出当前可用的服务提供者列表。
     * <p>
     * 该方法会根据路由规则和配置规则对原始的服务提供者列表进行过滤，
     * 返回过滤后的服务提供者列表。
     *
     * @param invocation 调用信息，可用于服务路由
     * @return 可用的服务提供者列表
     * @throws RpcException 当获取服务提供者列表失败时抛出此异常
     */
    List<Invoker<T>> list(Invocation invocation) throws RpcException;

}