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
package com.alibaba.dubbo.rpc.cluster.directory;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.Router;
import com.alibaba.dubbo.rpc.cluster.RouterFactory;
import com.alibaba.dubbo.rpc.cluster.router.MockInvokersSelector;
import com.alibaba.dubbo.rpc.cluster.router.tag.TagRouter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Directory的抽象实现类
 * 该类的主要职责是维护和管理Invoker列表，并结合Router进行服务路由
 * 
 * 主要特点：
 * 1. 通过list方法返回的Invoker列表都经过了Router的过滤
 * 2. 支持动态路由和静态路由的组合使用
 * 3. 提供了基础的URL管理和销毁机制
 */
public abstract class AbstractDirectory<T> implements Directory<T> {

    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(AbstractDirectory.class);

    // 服务URL，包含了服务的基本信息
    private final URL url;

    // 目录是否被销毁的标志
    private volatile boolean destroyed = false;

    // 消费者URL，包含了消费者端的配置信息
    private volatile URL consumerUrl;

    // 路由规则列表，用于对服务列表进行路由过滤
    private volatile List<Router> routers;

    public AbstractDirectory(URL url) {
        this(url, null);
    }

    public AbstractDirectory(URL url, List<Router> routers) {
        this(url, url, routers);
    }

    public AbstractDirectory(URL url, URL consumerUrl, List<Router> routers) {
        if (url == null)
            throw new IllegalArgumentException("url == null");
        this.url = url;
        this.consumerUrl = consumerUrl;
        setRouters(routers);
    }

    /**
     * 获取所有可用的服务列表
     * 该方法会进行如下操作：
     * 1. 检查目录是否已被销毁
     * 2. 调用doList获取原始服务列表
     * 3. 使用路由规则对服务列表进行过滤
     *
     * @param invocation 调用信息
     * @return 经过路由规则过滤后的Invoker列表
     * @throws RpcException 当目录已被销毁或路由过程出错时抛出异常
     */
    @Override
    public List<Invoker<T>> list(Invocation invocation) throws RpcException {
        if (destroyed) {
            throw new RpcException("Directory already destroyed .url: " + getUrl());
        }
        List<Invoker<T>> invokers = doList(invocation);
        List<Router> localRouters = this.routers; // local reference
        if (localRouters != null && !localRouters.isEmpty()) {
            for (Router router : localRouters) {
                try {
                    if (router.getUrl() == null || router.getUrl().getParameter(Constants.RUNTIME_KEY, false)) {
                        invokers = router.route(invokers, getConsumerUrl(), invocation);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
                }
            }
        }
        return invokers;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    public List<Router> getRouters() {
        return routers;
    }

    /**
     * 设置路由规则
     * 该方法会：
     * 1. 复制传入的路由规则列表
     * 2. 添加URL中配置的路由规则
     * 3. 添加Mock选择器和Tag路由器
     * 4. 对路由规则进行排序
     *
     * @param routers 路由规则列表
     */
    protected void setRouters(List<Router> routers) {
        // copy list
        routers = routers == null ? new ArrayList<Router>() : new ArrayList<Router>(routers);
        // append url router
        String routerkey = url.getParameter(Constants.ROUTER_KEY);
        if (routerkey != null && routerkey.length() > 0) {
            RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getExtension(routerkey);
            routers.add(routerFactory.getRouter(url));
        }
        // append mock invoker selector
        routers.add(new MockInvokersSelector());
        routers.add(new TagRouter());
        Collections.sort(routers);
        this.routers = routers;
    }

    public URL getConsumerUrl() {
        return consumerUrl;
    }

    public void setConsumerUrl(URL consumerUrl) {
        this.consumerUrl = consumerUrl;
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    /**
     * 销毁该目录
     * 将destroyed标志设置为true，标记该目录不再可用
     */
    @Override
    public void destroy() {
        destroyed = true;
    }

    /**
     * 获取原始的服务列表
     * 具体实现由子类完成，用于获取未经过路由规则过滤的原始服务列表
     *
     * @param invocation 调用信息
     * @return 原始的Invoker列表
     * @throws RpcException 当获取列表过程中出错时抛出异常
     */
    protected abstract List<Invoker<T>> doList(Invocation invocation) throws RpcException;

}
