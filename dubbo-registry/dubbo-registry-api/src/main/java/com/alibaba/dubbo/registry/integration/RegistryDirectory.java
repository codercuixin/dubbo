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
package com.alibaba.dubbo.registry.integration;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.Configurator;
import com.alibaba.dubbo.rpc.cluster.ConfiguratorFactory;
import com.alibaba.dubbo.rpc.cluster.Router;
import com.alibaba.dubbo.rpc.cluster.RouterFactory;
import com.alibaba.dubbo.rpc.cluster.directory.AbstractDirectory;
import com.alibaba.dubbo.rpc.cluster.directory.StaticDirectory;
import com.alibaba.dubbo.rpc.cluster.support.ClusterUtils;
import com.alibaba.dubbo.rpc.protocol.InvokerWrapper;
import com.alibaba.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 注册目录服务实现类，用于服务的动态发现。
 *
 * <p>主要特性：
 * <ul>
 * <li>动态服务发现 - 自动从注册中心发现和更新服务提供者</li>
 * <li>多协议支持 - 可以处理多种协议如dubbo、http、hessian等</li>
 * <li>配置器支持 - 通过覆盖规则允许动态配置服务提供者</li>
 * <li>路由支持 - 支持在不同服务提供者之间进行路由</li>
 * </ul>
 *
 * <p>目录维护的数据结构：
 * <ul>
 * <li>urlInvokerMap: URL到Invoker的映射缓存</li>
 * <li>methodInvokerMap: 方法名到Invoker列表的映射缓存</li>
 * <li>cachedInvokerUrls: 服务提供者URL的缓存</li>
 * </ul>
 *
 * <p>核心职责：
 * <ul>
 * <li>订阅注册中心事件并更新本地缓存</li>
 * <li>将服务提供者URL转换为Invoker</li>
 * <li>维护方法与Invoker之间的映射关系</li>
 * <li>支持服务路由和配置</li>
 * </ul>
 */
public class RegistryDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    // 集群处理器，用于合并多个服务提供者
    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    // 路由工厂，用于创建服务路由规则
    private static final RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getAdaptiveExtension();

    // 配置工厂，用于动态配置服务
    private static final ConfiguratorFactory configuratorFactory = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getAdaptiveExtension();
    
    // 服务键，在构造时初始化，不允许为空
    private final String serviceKey;
    
    // 服务类型，在构造时初始化，不允许为空
    private final Class<T> serviceType;
    
    // 查询参数映射，在构造时初始化，不允许为空
    private final Map<String, String> queryMap;
    
    // 目录URL，在构造时初始化，不允许为空，且始终赋非空值
    private final URL directoryUrl;
    
    // 服务方法数组
    private final String[] serviceMethods;
    
    // 是否为多分组服务
    private final boolean multiGroup;
    
    // 协议对象，在注入时初始化，不允许为空
    private Protocol protocol;
    
    // 注册中心对象，在注入时初始化，不允许为空
    private Registry registry;
    
    // 服务是否被禁用
    private volatile boolean forbidden = false;

    // 覆盖后的目录URL，在构造时初始化，不允许为空，且始终赋非空值
    private volatile URL overrideDirectoryUrl;

    // 已注册的消费者URL
    private volatile URL registeredConsumerUrl;

    /**
     * 覆盖规则配置器列表
     * 优先级顺序: override > -D > consumer > provider
     * 规则示例1: 针对特定提供者 <ip:port,timeout=100>
     * 规则示例2: 针对所有提供者 <* ,timeout=5000>
     * 
     * 注意：初始值为null，运行过程中可能被设为null，使用时请使用局部变量引用
     */
    private volatile List<Configurator> configurators;

    /**
     * URL到Invoker的映射缓存
     * 用于缓存服务URL到服务调用者的映射关系
     * 注意：初始值为null，运行过程中可能被设为null，使用时请使用局部变量引用
     */
    private volatile Map<String, Invoker<T>> urlInvokerMap;

    /**
     * 方法名到Invoker列表的映射缓存
     * 用于缓存服务方法到服务调用者列表的映射关系
     * 注意：初始值为null，运行过程中可能被设为null，使用时请使用局部变量引用
     */
    private volatile Map<String, List<Invoker<T>>> methodInvokerMap;

    /**
     * Invoker URL集合缓存
     * 用于缓存有效的服务提供者URL集合
     * 注意：初始值为null，运行过程中可能被设为null，使用时请使用局部变量引用
     */
    private volatile Set<URL> cachedInvokerUrls;

    /**
     * 注册目录服务构造函数
     * 
     * @param serviceType 服务接口类型
     * @param url 注册中心URL，包含服务查询和配置信息
     * @throws IllegalArgumentException 当服务类型为空或服务键为空时抛出
     */
    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(url);
        if (serviceType == null)
            throw new IllegalArgumentException("service type is null.");
        if (url.getServiceKey() == null || url.getServiceKey().length() == 0)
            throw new IllegalArgumentException("registry serviceKey is null.");
        this.serviceType = serviceType;
        this.serviceKey = url.getServiceKey();
        this.queryMap = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        this.overrideDirectoryUrl = this.directoryUrl = url.setPath(url.getServiceInterface()).clearParameters().addParameters(queryMap).removeParameter(Constants.MONITOR_KEY);
        String group = directoryUrl.getParameter(Constants.GROUP_KEY, "");
        this.multiGroup = group != null && ("*".equals(group) || group.contains(","));
        String methods = queryMap.get(Constants.METHODS_KEY);
        this.serviceMethods = methods == null ? null : Constants.COMMA_SPLIT_PATTERN.split(methods);
    }

    /**
     * 将覆盖规则URL转换为配置器列表，用于服务引用时的参数覆盖
     * 每次都会发送所有规则，URL会被重新组装和计算
     *
     * @param urls 覆盖规则URL列表，支持以下格式：
     *             </br>1. override://0.0.0.0/...( 或 override://ip:port...?anyhost=true)&para1=value1... 
     *                     表示全局规则（对所有提供者生效）
     *             </br>2. override://ip:port...?anyhost=false 
     *                     表示特殊规则（仅对某个特定提供者生效）
     *             </br>3. override:// 
     *                     不支持此规则格式，需要由注册中心自行计算
     *             </br>4. override://0.0.0.0/ 
     *                     不带参数表示清除覆盖规则
     * @return 配置器列表
     */
    public static List<Configurator> toConfigurators(List<URL> urls) {
        // 如果URL列表为空，返回空的配置器列表
        if (urls == null || urls.isEmpty()) {
            return Collections.emptyList();
        }

        // 创建配置器列表，预设容量为URL列表的大小
        List<Configurator> configurators = new ArrayList<Configurator>(urls.size());
        for (URL url : urls) {
            // 如果是空协议，清空配置器列表并结束循环
            // 这种情况表示需要清除所有的覆盖规则
            if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                configurators.clear();
                break;
            }
            // 将URL参数转换为Map，用于后续处理
            Map<String, String> override = new HashMap<String, String>(url.getParameters());
            // 移除anyhost参数，因为这个参数可能是自动添加的，不应影响URL的变更判断
            override.remove(Constants.ANYHOST_KEY);
            // 如果没有覆盖参数，清空配置器列表并继续下一个URL
            if (override.size() == 0) {
                configurators.clear();
                continue;
            }
            // 根据URL创建配置器并添加到列表中
            configurators.add(configuratorFactory.getConfigurator(url));
        }
        // 对配置器列表进行排序，确保优先级顺序
        Collections.sort(configurators);
        return configurators;
    }

    /**
     * 设置服务协议
     * 
     * @param protocol 服务协议实现
     */
    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    /**
     * 设置注册中心
     * 
     * @param registry 注册中心实现
     */
    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    /**
     * 订阅服务提供者
     * 设置消费者URL并向注册中心进行服务订阅
     * 
     * @param url 消费者URL
     */
    public void subscribe(URL url) {
        setConsumerUrl(url);
        registry.subscribe(url, this);
    }

    /**
     * 销毁注册目录服务
     * 包括取消注册消费者、取消订阅服务、销毁所有服务调用者等
     */
    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }

        // unregister.
        try {
            if (getRegisteredConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unregister(getRegisteredConsumerUrl());
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unregister service " + serviceKey + "from registry" + registry.getUrl(), t);
        }

        // unsubscribe.
        try {
            if (getConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getConsumerUrl(), this);
            }
        } catch (Throwable t) {
            logger.warn("unexpeced error when unsubscribe service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        super.destroy(); // must be executed after unsubscribing
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
    }

    /**
     * 处理注册中心的服务更新通知事件
     *
     * <p>此方法处理三种类型的通知：
     * <ul>
     * <li>配置器URLs - 用于动态配置更新</li>
     * <li>路由URLs - 用于路由规则更新</li>
     * <li>服务提供者URLs - 用于提供者列表更新</li>
     * </ul>
     *
     * <p>处理流程：
     * <ol>
     * <li>将URLs分类为配置器、路由器和服务提供者</li>
     * <li>如果存在配置器URLs，则更新配置器</li>
     * <li>如果存在路由器URLs，则更新路由器</li>
     * <li>合并来自配置器的覆盖参数</li>
     * <li>基于服务提供者URLs刷新调用者</li>
     * </ol>
     *
     * <p>特殊情况处理：
     * <ul>
     * <li>如果提供者列表为空且协议为empty，则禁止访问</li>
     * <li>如果没有通知URLs，则重用缓存的调用者URLs</li>
     * <li>无效的分类URLs将以警告形式记录</li>
     * </ul>
     *
     * @param urls 配置器、路由器和服务提供者的URL列表
     */
    @Override
    public synchronized void notify(List<URL> urls) {
        // 将URL分类为三种：服务提供者、路由规则、配置规则
        List<URL> invokerUrls = new ArrayList<URL>();    // 服务提供者URL列表
        List<URL> routerUrls = new ArrayList<URL>();     // 路由规则URL列表
        List<URL> configuratorUrls = new ArrayList<URL>();// 配置规则URL列表

        // 遍历URL列表，根据category和protocol进行分类
        for (URL url : urls) {
            String protocol = url.getProtocol();
            String category = url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
            // 路由规则URL
            if (Constants.ROUTERS_CATEGORY.equals(category)
                    || Constants.ROUTE_PROTOCOL.equals(protocol)) {
                routerUrls.add(url);
            } 
            // 配置规则URL
            else if (Constants.CONFIGURATORS_CATEGORY.equals(category)
                    || Constants.OVERRIDE_PROTOCOL.equals(protocol)) {
                configuratorUrls.add(url);
            } 
            // 服务提供者URL
            else if (Constants.PROVIDERS_CATEGORY.equals(category)) {
                invokerUrls.add(url);
            } else {
                logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
            }
        }

        // 处理配置规则：转换为Configurator列表
        if (configuratorUrls != null && !configuratorUrls.isEmpty()) {
            this.configurators = toConfigurators(configuratorUrls);
        }

        // 处理路由规则：转换为Router列表并设置
        if (routerUrls != null && !routerUrls.isEmpty()) {
            List<Router> routers = toRouters(routerUrls);
            if (routers != null) { // 如果为null则不做处理
                setRouters(routers);
            }
        }

        // 获取配置器的本地引用，提高线程安全性
        List<Configurator> localConfigurators = this.configurators;
        
        // 合并覆盖参数：将配置规则应用到目录URL
        this.overrideDirectoryUrl = directoryUrl;
        if (localConfigurators != null && !localConfigurators.isEmpty()) {
            for (Configurator configurator : localConfigurators) {
                // 依次应用每个配置器的规则
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }

        // 刷新服务提供者：根据最新的invokerUrls刷新Invoker列表
        refreshInvoker(invokerUrls);
    }

    /**
     * 根据给定的调用者URL列表刷新调用者列表
     *
     * <p>此方法处理三种情况：
     * <ol>
     * <li>禁止访问：当URL列表仅包含一个空协议URL时</li>
     * <li>无新URLs：重用缓存的调用者URLs</li>
     * <li>正常情况：将新URLs转换为调用者</li>
     * </ol>
     *
     * <p>正常情况的处理步骤：
     * <ol>
     * <li>将URLs转换为新的调用者映射</li>
     * <li>将调用者映射转换为方法调用者映射</li>
     * <li>使用新映射更新状态</li>
     * <li>销毁未使用的调用者</li>
     * </ol>
     *
     * <p>实现细节：
     * <ul>
     * <li>当收到单个空协议URL时，表示应该禁止访问的特殊情况</li>
     * <li>如果没有收到新URLs但存在缓存的URLs，将重用缓存的URLs</li>
     * <li>methodInvokerMap同时维护特定方法的调用者和任意方法的默认调用者列表</li>
     * <li>对于多分组场景，使用集群连接操作合并不同组的调用者</li>
     * <li>通过使用共享映射的本地引用确保线程安全</li>
     * <li>销毁未使用的调用者以防止内存泄漏</li>
     * </ul>
     *
     * <p>状态管理：
     * <ul>
     * <li>forbidden：控制是否允许服务访问</li>
     * <li>methodInvokerMap：缓存方法到调用者的映射</li>
     * <li>urlInvokerMap：缓存URL到调用者的映射</li>
     * <li>cachedInvokerUrls：存储有效的提供者URLs</li>
     * </ul>
     *
     * @param invokerUrls 调用者URL列表
     */
    // TODO: 2017/8/31 FIXME The thread pool should be used to refresh the address, otherwise the task may be accumulated.
    private void refreshInvoker(List<URL> invokerUrls) {
        // 特殊情况处理：如果只有一个空协议的URL，表示禁止访问
        if (invokerUrls != null && invokerUrls.size() == 1 && invokerUrls.get(0) != null
                && Constants.EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
            // 设置禁止访问标志
            this.forbidden = true; // Forbid to access
            this.methodInvokerMap = null; // Set the method invoker map to null
            destroyAllInvokers(); // Close all invokers
        } else {
            // 正常情况处理
            this.forbidden = false; // Allow to access

            // 获取当前的URL-Invoker映射作为本地引用，提高线程安全性
            Map<String, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap; // local reference

            // 如果没有新的invoker URLs，但存在缓存的URLs，则重用缓存的URLs
            if (invokerUrls.isEmpty() && this.cachedInvokerUrls != null) {
                invokerUrls.addAll(this.cachedInvokerUrls);
            } else {
                this.cachedInvokerUrls = new HashSet<URL>();
                this.cachedInvokerUrls.addAll(invokerUrls);//Cached invoker urls, convenient for comparison
            }

            if (invokerUrls.isEmpty()) {
                return;
            }

            // 将URL列表转换为Invoker映射
            Map<String, Invoker<T>> newUrlInvokerMap = toInvokers(invokerUrls);

            // 将Invoker映射转换为方法级别的Invoker映射
            Map<String, List<Invoker<T>>> newMethodInvokerMap = toMethodInvokers(newUrlInvokerMap);

            // 状态变更检查
            // 如果转换后的Invoker为空，可能存在异常
            if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" + invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls.toString()));
                return;
            }

            // 更新本地缓存
            // 如果是多组服务，需要合并多个组的Invoker列表
            this.methodInvokerMap = multiGroup ? toMergeMethodInvokerMap(newMethodInvokerMap) : newMethodInvokerMap;
            this.urlInvokerMap = newUrlInvokerMap;

            try {
                // 清理不再使用的Invoker，防止内存泄漏
                destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap);
            } catch (Exception e) {
                logger.warn("destroyUnusedInvokers error. ", e);
            }
        }
    }

    private Map<String, List<Invoker<T>>> toMergeMethodInvokerMap(Map<String, List<Invoker<T>>> methodMap) {
        Map<String, List<Invoker<T>>> result = new HashMap<String, List<Invoker<T>>>();
        for (Map.Entry<String, List<Invoker<T>>> entry : methodMap.entrySet()) {
            String method = entry.getKey();
            List<Invoker<T>> invokers = entry.getValue();
            Map<String, List<Invoker<T>>> groupMap = new HashMap<String, List<Invoker<T>>>();
            for (Invoker<T> invoker : invokers) {
                String group = invoker.getUrl().getParameter(Constants.GROUP_KEY, "");
                List<Invoker<T>> groupInvokers = groupMap.get(group);
                if (groupInvokers == null) {
                    groupInvokers = new ArrayList<Invoker<T>>();
                    groupMap.put(group, groupInvokers);
                }
                groupInvokers.add(invoker);
            }
            if (groupMap.size() == 1) {
                result.put(method, groupMap.values().iterator().next());
            } else if (groupMap.size() > 1) {
                List<Invoker<T>> groupInvokers = new ArrayList<Invoker<T>>();
                for (List<Invoker<T>> groupList : groupMap.values()) {
                    groupInvokers.add(cluster.join(new StaticDirectory<T>(groupList)));
                }
                result.put(method, groupInvokers);
            } else {
                result.put(method, invokers);
            }
        }
        return result;
    }

    /**
     * @param urls
     * @return null : no routers ,do nothing
     * else :routers list
     */
    private List<Router> toRouters(List<URL> urls) {
        List<Router> routers = new ArrayList<Router>();
        if (urls == null || urls.isEmpty()) {
            return routers;
        }
        if (urls != null && !urls.isEmpty()) {
            for (URL url : urls) {
                if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                    continue;
                }
                String routerType = url.getParameter(Constants.ROUTER_KEY);
                if (routerType != null && routerType.length() > 0) {
                    url = url.setProtocol(routerType);
                }
                try {
                    Router router = routerFactory.getRouter(url);
                    if (!routers.contains(router))
                        routers.add(router);
                } catch (Throwable t) {
                    logger.error("convert router url to router error, url: " + url, t);
                }
            }
        }
        return routers;
    }

    /**
     * 将URL列表转换为调用者映射
     * 
     * <p>转换过程：
     * <ol>
     * <li>如果queryMap中指定了协议，则按协议过滤URLs</li>
     * <li>跳过空协议URLs</li>
     * <li>验证协议是否被支持</li>
     * <li>按优先级合并URL参数</li>
     * <li>如果不在缓存中，则从URL创建调用者</li>
     * </ol>
     *
     * <p>实现细节：
     * <ul>
     * <li>协议过滤：仅处理与引用URL中指定协议匹配的URLs</li>
     * <li>协议验证：通过检查扩展加载器确保协议被支持</li>
     * <li>URL去重：使用完整URL字符串作为键以避免重复处理</li>
     * <li>缓存机制：重用现有调用者以避免不必要的重新创建</li>
     * <li>调用者状态：在创建新调用者前检查禁用/启用状态</li>
     * </ul>
     *
     * <p>调用者创建的关键步骤：
     * <ul>
     * <li>URL合并：合并来自多个来源的参数</li>
     * <li>状态检查：验证调用者是否启用</li>
     * <li>协议引用：创建底层协议调用者</li>
     * <li>委托包装：使用InvokerDelegate包装协议调用者以添加额外元数据</li>
     * </ul>
     *
     * @param urls 服务提供者URL列表
     * @return URL字符串到调用者的映射
     */
    private Map<String, Invoker<T>> toInvokers(List<URL> urls) {
        Map<String, Invoker<T>> newUrlInvokerMap = new HashMap<String, Invoker<T>>();
        if (urls == null || urls.isEmpty()) {
            return newUrlInvokerMap;
        }
        Set<String> keys = new HashSet<String>();
        // 获取引用配置中的协议
        String queryProtocols = this.queryMap.get(Constants.PROTOCOL_KEY);
        
        for (URL providerUrl : urls) {
            // 检查引用配置的协议是否匹配，不匹配则跳过
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;
                String[] acceptProtocols = queryProtocols.split(",");
                for (String acceptProtocol : acceptProtocols) {
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }
                if (!accept) {
                    continue;
                }
            }
            
            // 跳过空协议的URL
            if (Constants.EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                continue;
            }
            
            // 检查协议是否被支持
            if (!ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() + " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost()
                        + ", supported protocol: " + ExtensionLoader.getExtensionLoader(Protocol.class).getSupportedExtensions()));
                continue;
            }
            
            // 合并URL参数，按照优先级：override > -D > consumer > provider
            URL url = mergeUrl(providerUrl);

            // 使用完整的URL字符串作为键，确保URL的唯一性
            String key = url.toFullString(); // The parameter urls are sorted
            if (keys.contains(key)) { // 避免重复URL
                continue;
            }
            keys.add(key);
            
            // 尝试从缓存中获取invoker，如果服务端URL发生变化，则重新引用
            Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
            Invoker<T> invoker = localUrlInvokerMap == null ? null : localUrlInvokerMap.get(key);
            
            // 如果缓存中不存在，则创建新的invoker
            if (invoker == null) {
                try {
                    // 检查是否禁用
                    boolean enabled = true;
                    if (url.hasParameter(Constants.DISABLED_KEY)) {
                        enabled = !url.getParameter(Constants.DISABLED_KEY, false);
                    } else {
                        enabled = url.getParameter(Constants.ENABLED_KEY, true);
                    }
                    if (enabled) {
                        //这句话最关键：创建新的invoker实例
                        //1. protocol.refer: 通过SPI机制调用具体的协议实现来引用远程服务
                        //2. InvokerDelegate: 包装原始invoker，添加URL元数据
                        invoker = new InvokerDelegate<T>(protocol.refer(serviceType, url), url, providerUrl);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }
                if (invoker != null) { // 将新的invoker加入缓存
                    newUrlInvokerMap.put(key, invoker);
                }
            } else { // 如果缓存中存在，直接复用
                newUrlInvokerMap.put(key, invoker);
            }
        }
        keys.clear();
        return newUrlInvokerMap;
    }

    /**
     * Merge URL parameters according to the priority.
     *
     * <p>Priority order: override > -D > Consumer > Provider
     *
     * <p>The process:
     * <ol>
     * <li>Merge consumer side parameters</li>
     * <li>Apply configurators if present</li>
     * <li>Add check=false parameter</li>
     * <li>Merge provider side parameters</li>
     * <li>Handle compatibility for dubbo 1.0</li>
     * </ol>
     *
     * @param providerUrl The provider URL
     * @return The merged URL
     */
    private URL mergeUrl(URL providerUrl) {
        providerUrl = ClusterUtils.mergeUrl(providerUrl, queryMap); // Merge the consumer side parameters

        List<Configurator> localConfigurators = this.configurators; // local reference
        if (localConfigurators != null && !localConfigurators.isEmpty()) {
            for (Configurator configurator : localConfigurators) {
                providerUrl = configurator.configure(providerUrl);
            }
        }

        providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false)); // Do not check whether the connection is successful or not, always create Invoker!

        // The combination of directoryUrl and override is at the end of notify, which can't be handled here
        this.overrideDirectoryUrl = this.overrideDirectoryUrl.addParametersIfAbsent(providerUrl.getParameters()); // Merge the provider side parameters

        if ((providerUrl.getPath() == null || providerUrl.getPath().length() == 0)
                && "dubbo".equals(providerUrl.getProtocol())) { // Compatible version 1.0
            //fix by tony.chenl DUBBO-44
            String path = directoryUrl.getParameter(Constants.INTERFACE_KEY);
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        return providerUrl;
    }

    /**
     * 使用指定的方法对给定的调用者进行路由
     *
     * <p>对于每个路由器：
     * <ul>
     * <li>跳过运行时路由器</li>
     * <li>应用路由规则过滤调用者</li>
     * </ul>
     *
     * <p>路由处理说明：
     * <ul>
     * <li>创建RPC调用（不含参数）用于路由规则匹配</li>
     * <li>获取当前可用的路由器列表</li>
     * <li>对于每个非运行时路由器，应用其路由规则</li>
     * <li>路由结果可能减少调用者数量，但不会增加新的调用者</li>
     * </ul>
     *
     * @param invokers 要进行路由的调用者列表
     * @param method 要路由的方法名
     * @return 经过路由筛选后的调用者列表
     */
    private List<Invoker<T>> route(List<Invoker<T>> invokers, String method) {
        Invocation invocation = new RpcInvocation(method, new Class<?>[0], new Object[0]);
        List<Router> routers = getRouters();
        if (routers != null) {
            for (Router router : routers) {
                // If router's url not null and is not route by runtime,we filter invokers here
                if (router.getUrl() != null && !router.getUrl().getParameter(Constants.RUNTIME_KEY, false)) {
                    invokers = router.route(invokers, getConsumerUrl(), invocation);
                }
            }
        }
        return invokers;
    }

    /**
     * 将调用者映射转换为方法调用者映射
     * 
     * <p>转换遵循以下规则：
     * <ol>
     * <li>从调用者URL参数中提取方法</li>
     * <li>按方法名对调用者进行分组</li>
     * <li>为每个方法路由调用者</li>
     * <li>添加任意方法的默认路由</li>
     * <li>排序并设置为不可修改</li>
     * </ol>
     *
     * <p>实现细节：
     * <ul>
     * <li>方法提取：从提供者URL读取'methods'参数</li>
     * <li>方法分组：为每个方法创建独立的调用者列表</li>
     * <li>默认处理：为未定义的方法维护默认调用者列表</li>
     * <li>路由：为每个方法应用路由规则过滤调用者</li>
     * <li>线程安全：返回不可修改的集合以防止并发修改</li>
     * </ul>
     *
     * <p>特殊处理：
     * <ul>
     * <li>ANY_VALUE：用于未明确定义方法的默认调用者列表</li>
     * <li>空方法：回退到默认调用者列表</li>
     * <li>方法路由：每个方法获取自己的过滤后的调用者列表</li>
     * <li>排序：调用者按URL排序以确保一致的顺序</li>
     * </ul>
     *
     * @param invokersMap URL到调用者的映射
     * @return 方法名到调用者列表的映射
     */
    private Map<String, List<Invoker<T>>> toMethodInvokers(Map<String, Invoker<T>> invokersMap) {
        // 创建新的方法级别 Invoker 映射
        Map<String, List<Invoker<T>>> newMethodInvokerMap = new HashMap<String, List<Invoker<T>>>();
        // 创建 Invoker 列表，用于存储所有的 Invoker
        List<Invoker<T>> invokersList = new ArrayList<Invoker<T>>();
        
        if (invokersMap != null && invokersMap.size() > 0) {
            // 遍历所有的 Invoker
            for (Invoker<T> invoker : invokersMap.values()) {
                // 从 URL 中获取 methods 参数，这里包含了服务提供者支持的所有方法
                String parameter = invoker.getUrl().getParameter(Constants.METHODS_KEY);
                if (parameter != null && parameter.length() > 0) {
                    // 将方法名按逗号分割
                    String[] methods = Constants.COMMA_SPLIT_PATTERN.split(parameter);
                    if (methods != null && methods.length > 0) {
                        // 遍历所有方法名
                        for (String method : methods) {
                            // 检查方法名的有效性，排除空方法名和通配符
                            if (method != null && method.length() > 0
                                    && !Constants.ANY_VALUE.equals(method)) {
                                // 获取该方法对应的 Invoker 列表，如果不存在则创建新的
                                List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                                if (methodInvokers == null) {
                                    methodInvokers = new ArrayList<Invoker<T>>();
                                    newMethodInvokerMap.put(method, methodInvokers);
                                }
                                // 将当前 Invoker 添加到方法对应的列表中
                                methodInvokers.add(invoker);
                            }
                        }
                    }
                }
                // 将当前 Invoker 添加到总列表中
                invokersList.add(invoker);
            }
        }
        
        // 对所有 Invoker 进行路由过滤，得到默认的 Invoker 列表
        List<Invoker<T>> newInvokersList = route(invokersList, null);
        // 将默认的 Invoker 列表放入 ANY_VALUE 键中，作为未指定方法的默认列表
        newMethodInvokerMap.put(Constants.ANY_VALUE, newInvokersList);
        
        // 如果接口中定义了方法（通过 serviceMethods 指定）
        if (serviceMethods != null && serviceMethods.length > 0) {
            for (String method : serviceMethods) {
                // 获取方法对应的 Invoker 列表
                List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                // 如果方法没有对应的 Invoker 列表，使用默认列表
                if (methodInvokers == null || methodInvokers.isEmpty()) {
                    methodInvokers = newInvokersList;
                }
                // 对方法对应的 Invoker 列表进行路由过滤，并更新到映射中
                newMethodInvokerMap.put(method, route(methodInvokers, method));
            }
        }
        
        // 最终处理：排序并设置为不可修改
        for (String method : new HashSet<String>(newMethodInvokerMap.keySet())) {
            List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
            // 按 URL 对 Invoker 进行排序，确保顺序一致性
            Collections.sort(methodInvokers, InvokerComparator.getComparator());
            // 将列表设置为不可修改，保证线程安全
            newMethodInvokerMap.put(method, Collections.unmodifiableList(methodInvokers));
        }
        // 将整个映射设置为不可修改，返回
        return Collections.unmodifiableMap(newMethodInvokerMap);
    }

    /**
     * 销毁所有的服务调用者
     * 
     * <p>销毁过程：
     * <ul>
     * <li>获取当前URL-调用者映射的本地引用</li>
     * <li>遍历所有调用者并执行销毁操作</li>
     * <li>清空URL-调用者映射</li>
     * <li>清空方法-调用者映射</li>
     * </ul>
     * 
     * <p>注意：销毁过程中的异常会被记录但不会中断流程
     */
    private void destroyAllInvokers() {
        Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
        if (localUrlInvokerMap != null) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                try {
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
            }
            localUrlInvokerMap.clear();
        }
        methodInvokerMap = null;
    }

    /**
     * 销毁旧调用者映射中未使用的调用者
     *
     * <p>这可以防止不再需要的调用者造成内存泄漏。
     * 如果refer.autodestroy=false，调用者数量只会增加而不会减少。
     *
     * @param oldUrlInvokerMap 旧的调用者映射
     * @param newUrlInvokerMap 新的调用者映射
     */
    private void destroyUnusedInvokers(Map<String, Invoker<T>> oldUrlInvokerMap, Map<String, Invoker<T>> newUrlInvokerMap) {
        if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
            destroyAllInvokers();
            return;
        }
        // check deleted invoker
        List<String> deleted = null;
        if (oldUrlInvokerMap != null) {
            Collection<Invoker<T>> newInvokers = newUrlInvokerMap.values();
            for (Map.Entry<String, Invoker<T>> entry : oldUrlInvokerMap.entrySet()) {
                if (!newInvokers.contains(entry.getValue())) {
                    if (deleted == null) {
                        deleted = new ArrayList<String>();
                    }
                    deleted.add(entry.getKey());
                }
            }
        }

        if (deleted != null) {
            for (String url : deleted) {
                if (url != null) {
                    Invoker<T> invoker = oldUrlInvokerMap.remove(url);
                    if (invoker != null) {
                        try {
                            invoker.destroy();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destroy invoker[" + invoker.getUrl() + "] success. ");
                            }
                        } catch (Exception e) {
                            logger.warn("destroy invoker[" + invoker.getUrl() + "] faild. " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 获取调用者列表
     * 
     * <p>查找过程：
     * <ul>
     * <li>检查服务是否被禁用</li>
     * <li>获取方法名和参数</li>
     * <li>按优先级查找调用者列表：
     *   <ol>
     *   <li>先查找方法名+第一个参数的特定调用者</li>
     *   <li>再查找方法名对应的调用者</li>
     *   <li>最后查找默认调用者</li>
     *   </ol>
     * </li>
     * </ul>
     *
     * @param invocation RPC调用信息
     * @return 可用的调用者列表，如果没有找到则返回空列表
     * @throws RpcException 当服务被禁用时抛出
     */
    @Override
    public List<Invoker<T>> doList(Invocation invocation) {
        if (forbidden) {
            // 1. No service provider 2. Service providers are disabled
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION,
                "No provider available from registry " + getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " +  NetUtils.getLocalHost()
                        + " use dubbo version " + Version.getVersion() + ", please check status of providers(disabled, not registered or in blacklist).");
        }
        List<Invoker<T>> invokers = null;
        Map<String, List<Invoker<T>>> localMethodInvokerMap = this.methodInvokerMap; // local reference
        if (localMethodInvokerMap != null && localMethodInvokerMap.size() > 0) {
            String methodName = RpcUtils.getMethodName(invocation);
            Object[] args = RpcUtils.getArguments(invocation);
            if (args != null && args.length > 0 && args[0] != null
                    && (args[0] instanceof String || args[0].getClass().isEnum())) {
                invokers = localMethodInvokerMap.get(methodName + "." + args[0]); // The routing can be enumerated according to the first parameter
            }
            if (invokers == null) {
                invokers = localMethodInvokerMap.get(methodName);
            }
            if (invokers == null) {
                invokers = localMethodInvokerMap.get(Constants.ANY_VALUE);
            }
            if (invokers == null) {
                Iterator<List<Invoker<T>>> iterator = localMethodInvokerMap.values().iterator();
                if (iterator.hasNext()) {
                    invokers = iterator.next();
                }
            }
        }
        return invokers == null ? new ArrayList<Invoker<T>>(0) : invokers;
    }

    /**
     * 获取服务接口类型
     *
     * @return 服务接口的Class对象
     */
    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    /**
     * 获取覆盖后的目录URL
     *
     * @return 目录URL
     */
    @Override
    public URL getUrl() {
        return this.overrideDirectoryUrl;
    }

    /**
     * 获取已注册的消费者URL
     *
     * @return 消费者URL
     */
    public URL getRegisteredConsumerUrl() {
        return registeredConsumerUrl;
    }

    /**
     * 设置已注册的消费者URL
     *
     * @param registeredConsumerUrl 消费者URL
     */
    public void setRegisteredConsumerUrl(URL registeredConsumerUrl) {
        this.registeredConsumerUrl = registeredConsumerUrl;
    }

    /**
     * 检查服务是否可用
     * 
     * <p>可用性检查：
     * <ul>
     * <li>检查服务是否已销毁</li>
     * <li>检查是否存在调用者映射</li>
     * <li>遍历所有调用者检查是否有可用的调用者</li>
     * </ul>
     *
     * @return 如果有任何一个调用者可用则返回true，否则返回false
     */
    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<String, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        if (localUrlInvokerMap != null && localUrlInvokerMap.size() > 0) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                if (invoker.isAvailable()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 获取URL到调用者的映射关系
     * 注意：此方法主要用于测试目的
     *
     * @return URL到调用者的映射
     */
    public Map<String, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    /**
     * 获取方法到调用者列表的映射关系
     * 注意：此方法主要用于测试目的
     *
     * @return 方法到调用者列表的映射
     */
    public Map<String, List<Invoker<T>>> getMethodInvokerMap() {
        return methodInvokerMap;
    }

    /**
     * 调用者比较器，用于对调用者进行排序
     * 采用单例模式实现，通过比较调用者的URL字符串进行排序
     */
    private static class InvokerComparator implements Comparator<Invoker<?>> {

        /**
         * 比较器单例
         */
        private static final InvokerComparator comparator = new InvokerComparator();

        /**
         * 私有构造函数，防止外部实例化
         */
        private InvokerComparator() {
        }

        /**
         * 获取比较器实例
         *
         * @return 比较器单例
         */
        public static InvokerComparator getComparator() {
            return comparator;
        }

        /**
         * 比较两个调用者
         * 通过比较它们的URL字符串进行排序
         *
         * @param o1 第一个调用者
         * @param o2 第二个调用者
         * @return 比较结果
         */
        @Override
        public int compare(Invoker<?> o1, Invoker<?> o2) {
            return o1.getUrl().toString().compareTo(o2.getUrl().toString());
        }

    }

    /**
     * 调用者委托类，主要用于存储注册中心发送的URL地址
     * 可以基于providerURL、queryMap和overrideMap重新组装用于重新引用
     *
     * @param <T> 服务接口类型
     */
    private static class InvokerDelegate<T> extends InvokerWrapper<T> {
        private URL providerUrl;

        public InvokerDelegate(Invoker<T> invoker, URL url, URL providerUrl) {
            super(invoker, url);
            this.providerUrl = providerUrl;
        }

        public URL getProviderUrl() {
            return providerUrl;
        }
    }
}
