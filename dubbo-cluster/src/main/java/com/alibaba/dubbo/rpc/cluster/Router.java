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
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;

import java.util.List;

/**
 * 路由规则接口。(SPI, Prototype, ThreadSafe)
 * <p>
 * 路由规则用于在服务调用过程中进行服务提供者的过滤，主要用途包括：
 * <ul>
 * <li>条件路由：根据调用方和提供方的参数进行路由</li>
 * <li>标签路由：根据服务标签进行路由</li>
 * <li>脚本路由：使用脚本语言定义路由规则</li>
 * </ul>
 * <p>
 * 路由规则可以按优先级排序，优先级高的路由规则优先执行。
 * <p>
 * 相关链接：
 * <a href="http://en.wikipedia.org/wiki/Routing">路由</a>
 *
 * @see com.alibaba.dubbo.rpc.cluster.Cluster#join(Directory)
 * @see com.alibaba.dubbo.rpc.cluster.Directory#list(Invocation)
 */
public interface Router extends Comparable<Router>{

    /**
     * 获取路由规则的URL。
     * 
     * @return 路由规则的URL，包含了路由规则的配置信息
     */
    URL getUrl();

    /**
     * 根据路由规则进行服务提供者的过滤。
     *
     * @param invokers   服务提供者列表
     * @param url        服务引用URL，包含了路由规则相关的配置信息
     * @param invocation 调用信息，可用于参与路由决策
     * @return 经过路由规则过滤后的服务提供者列表
     * @throws RpcException 当路由过程中发生错误时抛出此异常
     */
    <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException;

    /**
     * 获取路由规则的优先级。
     * <p>
     * 优先级用于对多个路由规则进行排序，数值越大优先级越高。
     * 当存在多个路由规则时，优先级高的路由规则将优先执行。
     *
     * @return 路由规则的优先级
     */
    int getPriority();

}