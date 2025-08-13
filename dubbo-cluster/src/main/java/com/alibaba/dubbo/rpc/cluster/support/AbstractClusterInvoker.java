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
package com.alibaba.dubbo.rpc.cluster.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;
import com.alibaba.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 集群调用器的抽象实现类。
 * <p>
 * 该类实现了集群调用的核心逻辑，包括：
 * <ul>
 * <li>服务目录管理：维护服务提供者列表</li>
 * <li>粘滞连接：支持将请求粘滞到同一个服务提供者</li>
 * <li>可用性检查：在调用前检查服务提供者是否可用</li>
 * <li>负载均衡：使用负载均衡策略选择服务提供者</li>
 * <li>失败重试：支持调用失败后重新选择服务提供者</li>
 * </ul>
 * <p>
 * 该类是一个模板类，具体的调用策略由子类实现，常见的实现包括：
 * <ul>
 * <li>FailoverClusterInvoker - 失败自动切换</li>
 * <li>FailfastClusterInvoker - 快速失败</li>
 * <li>FailsafeClusterInvoker - 失败安全</li>
 * <li>FailbackClusterInvoker - 失败自动恢复</li>
 * <li>ForkingClusterInvoker - 并行调用</li>
 * </ul>
 * 
 * @param <T> 服务接口类型
 */
public abstract class AbstractClusterInvoker<T> implements Invoker<T> {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory
            .getLogger(AbstractClusterInvoker.class);

    /**
     * 服务目录，维护了所有服务提供者列表
     */
    protected final Directory<T> directory;

    /**
     * 是否在调用时检查服务提供者是否可用
     */
    protected final boolean availablecheck;

    /**
     * 标记该调用器是否已被销毁
     */
    private AtomicBoolean destroyed = new AtomicBoolean(false);

    /**
     * 粘滞连接的服务提供者。
     * <p>
     * 如果开启了粘滞连接特性（sticky=true），则会将调用请求发送到同一个服务提供者。
     * 粘滞连接特性仅在服务提供者可用且未被选择时启用。
     */
    private volatile Invoker<T> stickyInvoker = null;

    /**
     * 使用服务目录构造集群调用器。
     *
     * @param directory 服务目录
     */
    public AbstractClusterInvoker(Directory<T> directory) {
        this(directory, directory.getUrl());
    }

    /**
     * 使用服务目录和URL构造集群调用器。
     *
     * @param directory 服务目录
     * @param url 服务URL，包含了集群调用的配置信息
     * @throws IllegalArgumentException 当服务目录为空时抛出此异常
     */
    public AbstractClusterInvoker(Directory<T> directory, URL url) {
        if (directory == null)
            throw new IllegalArgumentException("service directory == null");

        this.directory = directory;
        // 从URL中获取是否进行服务可用性检查的配置
        // 当availablecheck为true时，调用服务提供者之前必须先检查其是否可用
        this.availablecheck = url.getParameter(Constants.CLUSTER_AVAILABLE_CHECK_KEY, Constants.DEFAULT_CLUSTER_AVAILABLE_CHECK);
    }

    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    @Override
    public URL getUrl() {
        return directory.getUrl();
    }

    @Override
    public boolean isAvailable() {
        Invoker<T> invoker = stickyInvoker;
        if (invoker != null) {
            return invoker.isAvailable();
        }
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            directory.destroy();
        }
    }

    /**
     * 使用负载均衡策略选择一个服务提供者。
     * <p>
     * 选择过程包含以下步骤：
     * <ol>
     * <li>检查粘滞连接：如果启用了粘滞连接且上一次选择的服务提供者仍然可用，则继续使用该服务提供者</li>
     * <li>使用负载均衡策略选择：使用指定的负载均衡策略选择一个服务提供者</li>
     * <li>重新选择：如果选中的服务提供者在已选列表中或不可用，则进行重新选择</li>
     * </ol>
     * <p>
     * 重新选择的规则是：优先选择不在已选列表中且可用的服务提供者，这样可以：
     * <ul>
     * <li>保证所选的服务提供者尽量不在已选列表中</li>
     * <li>保证所选的服务提供者是可用的</li>
     * </ul>
     *
     * @param loadbalance 负载均衡策略
     * @param invocation 调用信息
     * @param invokers 候选的服务提供者列表
     * @param selected 已选的服务提供者列表（用于排除）
     * @return 选中的服务提供者
     * @throws RpcException 当选择过程中发生错误时抛出此异常
     */
    protected Invoker<T> select(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {
        if (invokers == null || invokers.isEmpty())
            return null;
        String methodName = invocation == null ? "" : invocation.getMethodName();

        boolean sticky = invokers.get(0).getUrl().getMethodParameter(methodName, Constants.CLUSTER_STICKY_KEY, Constants.DEFAULT_CLUSTER_STICKY);
        {
            //ignore overloaded method
            if (stickyInvoker != null && !invokers.contains(stickyInvoker)) {
                stickyInvoker = null;
            }
            //ignore concurrency problem
            if (sticky && stickyInvoker != null && (selected == null || !selected.contains(stickyInvoker))) {
                if (availablecheck && stickyInvoker.isAvailable()) {
                    return stickyInvoker;
                }
            }
        }
        Invoker<T> invoker = doSelect(loadbalance, invocation, invokers, selected);

        if (sticky) {
            stickyInvoker = invoker;
        }
        return invoker;
    }

    private Invoker<T> doSelect(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {
        if (invokers == null || invokers.isEmpty())
            return null;
        if (invokers.size() == 1)
            return invokers.get(0);
        if (loadbalance == null) {
            loadbalance = ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(Constants.DEFAULT_LOADBALANCE);
        }
        Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);

        //If the `invoker` is in the  `selected` or invoker is unavailable && availablecheck is true, reselect.
        if ((selected != null && selected.contains(invoker))
                || (!invoker.isAvailable() && getUrl() != null && availablecheck)) {
            try {
                Invoker<T> rinvoker = reselect(loadbalance, invocation, invokers, selected, availablecheck);
                if (rinvoker != null) {
                    invoker = rinvoker;
                } else {
                    //Check the index of current selected invoker, if it's not the last one, choose the one at index+1.
                    int index = invokers.indexOf(invoker);
                    try {
                        //Avoid collision
                        invoker = index < invokers.size() - 1 ? invokers.get(index + 1) : invokers.get(0);
                    } catch (Exception e) {
                        logger.warn(e.getMessage() + " may because invokers list dynamic change, ignore.", e);
                    }
                }
            } catch (Throwable t) {
                logger.error("cluster reselect fail reason is :" + t.getMessage() + " if can not solve, you can set cluster.availablecheck=false in url", t);
            }
        }
        return invoker;
    }

    /**
     * 重新选择服务提供者。
     * <p>
     * 重新选择的策略是：
     * <ol>
     * <li>优先选择不在已选列表中的可用服务提供者</li>
     * <li>如果所有服务提供者都在已选列表中，则使用负载均衡策略选择一个可用的服务提供者</li>
     * </ol>
     * <p>
     * 该方法主要用于以下场景：
     * <ul>
     * <li>失败重试：当调用失败时，重新选择一个服务提供者进行重试</li>
     * <li>并行调用：同时调用多个服务提供者，需要确保选择不同的服务提供者</li>
     * </ul>
     *
     * @param loadbalance 负载均衡策略
     * @param invocation 调用信息
     * @param invokers 候选的服务提供者列表
     * @param selected 已选的服务提供者列表
     * @param availablecheck 是否检查服务提供者的可用性
     * @return 重新选择的服务提供者，如果没有可用的服务提供者则返回null
     * @throws RpcException 当重新选择过程中发生错误时抛出此异常
     */
    private Invoker<T> reselect(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected, boolean availablecheck)
            throws RpcException {

        //Allocating one in advance, this list is certain to be used.
        List<Invoker<T>> reselectInvokers = new ArrayList<Invoker<T>>(invokers.size() > 1 ? (invokers.size() - 1) : invokers.size());

        //First, try picking a invoker not in `selected`.
        if (availablecheck) { // invoker.isAvailable() should be checked
            for (Invoker<T> invoker : invokers) {
                if (invoker.isAvailable()) {
                    if (selected == null || !selected.contains(invoker)) {
                        reselectInvokers.add(invoker);
                    }
                }
            }
            if (!reselectInvokers.isEmpty()) {
                return loadbalance.select(reselectInvokers, getUrl(), invocation);
            }
        } else { // do not check invoker.isAvailable()
            for (Invoker<T> invoker : invokers) {
                if (selected == null || !selected.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
            if (!reselectInvokers.isEmpty()) {
                return loadbalance.select(reselectInvokers, getUrl(), invocation);
            }
        }
        // Just pick an available invoker using loadbalance policy
        {
            if (selected != null) {
                for (Invoker<T> invoker : selected) {
                    if ((invoker.isAvailable()) // available first
                            && !reselectInvokers.contains(invoker)) {
                        reselectInvokers.add(invoker);
                    }
                }
            }
            if (!reselectInvokers.isEmpty()) {
                return loadbalance.select(reselectInvokers, getUrl(), invocation);
            }
        }
        return null;
    }

    /**
     * 执行远程调用。
     * <p>
     * 调用过程包括以下步骤：
     * <ol>
     * <li>检查集群调用器是否已被销毁</li>
     * <li>获取负载均衡策略</li>
     * <li>将RpcContext中的附加参数绑定到调用信息中</li>
     * <li>获取可用的服务提供者列表</li>
     * <li>执行具体的调用策略（由子类实现）</li>
     * </ol>
     *
     * @param invocation 调用信息，包含了方法名、参数等信息
     * @return 调用结果
     * @throws RpcException 当调用过程中发生错误时抛出此异常
     */
    @Override
    public Result invoke(final Invocation invocation) throws RpcException {
        checkWhetherDestroyed();
        LoadBalance loadbalance = null;

        // binding attachments into invocation.
        Map<String, String> contextAttachments = RpcContext.getContext().getAttachments();
        if (contextAttachments != null && contextAttachments.size() != 0) {
            ((RpcInvocation) invocation).addAttachments(contextAttachments);
        }

        List<Invoker<T>> invokers = list(invocation);
        if (invokers != null && !invokers.isEmpty()) {
            loadbalance = ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(invokers.get(0).getUrl()
                    .getMethodParameter(RpcUtils.getMethodName(invocation), Constants.LOADBALANCE_KEY, Constants.DEFAULT_LOADBALANCE));
        }
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);
        return doInvoke(invocation, invokers, loadbalance);
    }

    /**
     * 检查集群调用器是否已被销毁。
     *
     * @throws RpcException 如果集群调用器已被销毁则抛出此异常
     */
    protected void checkWhetherDestroyed() {
        if (destroyed.get()) {
            throw new RpcException("Rpc cluster invoker for " + getInterface() + " on consumer " + NetUtils.getLocalHost()
                    + " use dubbo version " + Version.getVersion()
                    + " is now destroyed! Can not invoke any more.");
        }
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + getUrl().toString();
    }

    /**
     * 检查服务提供者列表是否为空。
     * <p>
     * 如果服务提供者列表为空，则说明没有可用的服务提供者，
     * 此时会抛出异常，异常信息中包含了详细的错误原因。
     *
     * @param invokers 服务提供者列表
     * @param invocation 调用信息
     * @throws RpcException 当服务提供者列表为空时抛出此异常
     */
    protected void checkInvokers(List<Invoker<T>> invokers, Invocation invocation) {
        if (invokers == null || invokers.isEmpty()) {
            throw new RpcException("Failed to invoke the method "
                    + invocation.getMethodName() + " in the service " + getInterface().getName()
                    + ". No provider available for the service " + directory.getUrl().getServiceKey()
                    + " from registry " + directory.getUrl().getAddress()
                    + " on the consumer " + NetUtils.getLocalHost()
                    + " using the dubbo version " + Version.getVersion()
                    + ". Please check if the providers have been started and registered.");
        }
    }

    /**
     * 执行服务调用的模板方法。
     * <p>
     * 具体的调用策略由子类实现，不同的子类实现不同的调用策略：
     * <ul>
     * <li>FailoverClusterInvoker - 失败自动切换</li>
     * <li>FailfastClusterInvoker - 快速失败</li>
     * <li>FailsafeClusterInvoker - 失败安全</li>
     * <li>FailbackClusterInvoker - 失败自动恢复</li>
     * <li>ForkingClusterInvoker - 并行调用</li>
     * </ul>
     *
     * @param invocation 调用信息
     * @param invokers 可用的服务提供者列表
     * @param loadbalance 负载均衡策略
     * @return 调用结果
     * @throws RpcException 当调用过程中发生错误时抛出此异常
     */
    protected abstract Result doInvoke(Invocation invocation, List<Invoker<T>> invokers,
                                       LoadBalance loadbalance) throws RpcException;

    /**
     * 获取可用的服务提供者列表。
     * <p>
     * 该方法会从服务目录中获取符合当前调用要求的服务提供者列表。
     * 服务目录会根据路由规则和配置规则对服务提供者列表进行过滤。
     *
     * @param invocation 调用信息
     * @return 可用的服务提供者列表
     * @throws RpcException 当获取服务提供者列表失败时抛出此异常
     */
    protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
        List<Invoker<T>> invokers = directory.list(invocation);
        return invokers;
    }
}
