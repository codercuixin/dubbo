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
 * Registry directory implementation for dynamic discovery of services.
 *
 * <p>Features:
 * <ul>
 * <li>Dynamic service discovery - automatically discovers and updates service providers from registry</li>
 * <li>Multiple protocol support - can handle multiple protocols like dubbo, http, hessian etc</li>
 * <li>Configurator support - allows dynamic configuration of providers through override rules</li>
 * <li>Router support - supports routing between different service providers</li>
 * </ul>
 *
 * <p>The directory maintains:
 * <ul>
 * <li>urlInvokerMap: cache of URL to Invoker mapping</li>
 * <li>methodInvokerMap: cache of method name to list of Invokers mapping</li>
 * <li>cachedInvokerUrls: cache of provider URLs</li>
 * </ul>
 *
 * <p>Key responsibilities:
 * <ul>
 * <li>Subscribe to registry events and update local caches</li>
 * <li>Convert provider URLs to Invokers</li>
 * <li>Maintain mapping between methods and Invokers</li>
 * <li>Support service routing and configuration</li>
 * </ul>
 */
public class RegistryDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    private static final RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getAdaptiveExtension();

    private static final ConfiguratorFactory configuratorFactory = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getAdaptiveExtension();
    private final String serviceKey; // Initialization at construction time, assertion not null
    private final Class<T> serviceType; // Initialization at construction time, assertion not null
    private final Map<String, String> queryMap; // Initialization at construction time, assertion not null
    private final URL directoryUrl; // Initialization at construction time, assertion not null, and always assign non null value
    private final String[] serviceMethods;
    private final boolean multiGroup;
    private Protocol protocol; // Initialization at the time of injection, the assertion is not null
    private Registry registry; // Initialization at the time of injection, the assertion is not null
    private volatile boolean forbidden = false;

    private volatile URL overrideDirectoryUrl; // Initialization at construction time, assertion not null, and always assign non null value

    private volatile URL registeredConsumerUrl;

    /**
     * override rules
     * Priority: override>-D>consumer>provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     */
    private volatile List<Configurator> configurators; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Map<url, Invoker> cache service url to invoker mapping.
    private volatile Map<String, Invoker<T>> urlInvokerMap; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Map<methodName, Invoker> cache service method to invokers mapping.
    private volatile Map<String, List<Invoker<T>>> methodInvokerMap; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Set<invokerUrls> cache invokeUrls to invokers mapping.
    private volatile Set<URL> cachedInvokerUrls; // The initial value is null and the midway may be assigned to null, please use the local variable reference

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
     * Convert override urls to map for use when re-refer.
     * Send all rules every time, the urls will be reassembled and calculated
     *
     * @param urls Contract:
     *             </br>1.override://0.0.0.0/...( or override://ip:port...?anyhost=true)&para1=value1... means global rules (all of the providers take effect)
     *             </br>2.override://ip:port...?anyhost=false Special rules (only for a certain provider)
     *             </br>3.override:// rule is not supported... ,needs to be calculated by registry itself.
     *             </br>4.override://0.0.0.0/ without parameters means clearing the override
     * @return
     */
    public static List<Configurator> toConfigurators(List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return Collections.emptyList();
        }

        List<Configurator> configurators = new ArrayList<Configurator>(urls.size());
        for (URL url : urls) {
            if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                configurators.clear();
                break;
            }
            Map<String, String> override = new HashMap<String, String>(url.getParameters());
            //The anyhost parameter of override may be added automatically, it can't change the judgement of changing url
            override.remove(Constants.ANYHOST_KEY);
            if (override.size() == 0) {
                configurators.clear();
                continue;
            }
            configurators.add(configuratorFactory.getConfigurator(url));
        }
        Collections.sort(configurators);
        return configurators;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public void subscribe(URL url) {
        setConsumerUrl(url);
        registry.subscribe(url, this);
    }

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
     * Handles registry notification events for service updates.
     *
     * <p>This method processes three types of notifications:
     * <ul>
     * <li>Configurator URLs - for dynamic configuration updates</li>
     * <li>Router URLs - for routing rule updates</li>
     * <li>Provider URLs - for provider list updates</li>
     * </ul>
     *
     * <p>The processing flow:
     * <ol>
     * <li>Categorize URLs into configurators, routers and providers</li>
     * <li>Update configurators if configurator URLs present</li>
     * <li>Update routers if router URLs present</li>
     * <li>Merge override parameters from configurators</li>
     * <li>Refresh invokers based on provider URLs</li>
     * </ol>
     *
     * <p>Special cases:
     * <ul>
     * <li>If provider list is empty with protocol = empty, forbids access</li>
     * <li>If no notification URLs, reuse cached invoker URLs</li>
     * <li>Invalid category URLs are logged as warnings</li>
     * </ul>
     *
     * @param urls List of URLs for configurators, routers and providers
     */
    @Override
    public synchronized void notify(List<URL> urls) {
        List<URL> invokerUrls = new ArrayList<URL>();
        List<URL> routerUrls = new ArrayList<URL>();
        List<URL> configuratorUrls = new ArrayList<URL>();
        for (URL url : urls) {
            String protocol = url.getProtocol();
            String category = url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
            if (Constants.ROUTERS_CATEGORY.equals(category)
                    || Constants.ROUTE_PROTOCOL.equals(protocol)) {
                routerUrls.add(url);
            } else if (Constants.CONFIGURATORS_CATEGORY.equals(category)
                    || Constants.OVERRIDE_PROTOCOL.equals(protocol)) {
                configuratorUrls.add(url);
            } else if (Constants.PROVIDERS_CATEGORY.equals(category)) {
                invokerUrls.add(url);
            } else {
                logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
            }
        }
        // configurators
        if (configuratorUrls != null && !configuratorUrls.isEmpty()) {
            this.configurators = toConfigurators(configuratorUrls);
        }
        // routers
        if (routerUrls != null && !routerUrls.isEmpty()) {
            List<Router> routers = toRouters(routerUrls);
            if (routers != null) { // null - do nothing
                setRouters(routers);
            }
        }
        List<Configurator> localConfigurators = this.configurators; // local reference
        // merge override parameters
        this.overrideDirectoryUrl = directoryUrl;
        if (localConfigurators != null && !localConfigurators.isEmpty()) {
            for (Configurator configurator : localConfigurators) {
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }
        // providers
        refreshInvoker(invokerUrls);
    }

    /**
     * Refresh the invoker list from the given invoker URL list.
     *
     * <p>This method handles three cases:
     * <ol>
     * <li>Forbidden case: when URL list contains a single empty protocol URL</li>
     * <li>No new URLs: reuse cached invoker URLs</li>
     * <li>Normal case: convert new URLs to invokers</li>
     * </ol>
     *
     * <p>Steps for normal case:
     * <ol>
     * <li>Convert URLs to new invoker map</li>
     * <li>Convert invoker map to method invoker map</li>
     * <li>Update state with new maps</li>
     * <li>Destroy unused invokers</li>
     * </ol>
     *
     * <p>Implementation details:
     * <ul>
     * <li>When a single empty protocol URL is received, it indicates a special case where access should be forbidden</li>
     * <li>If no new URLs are received but cached URLs exist, the cached ones will be reused</li>
     * <li>The methodInvokerMap maintains both method-specific invokers and a default invoker list for any method</li>
     * <li>For multi-group scenarios, invokers from different groups are merged using the cluster join operation</li>
     * <li>Thread safety is ensured by using local references for shared maps</li>
     * <li>Unused invokers are destroyed to prevent memory leaks</li>
     * </ul>
     *
     * <p>State management:
     * <ul>
     * <li>forbidden: controls whether service access is allowed</li>
     * <li>methodInvokerMap: caches method-to-invoker mappings</li>
     * <li>urlInvokerMap: caches URL-to-invoker mappings</li>
     * <li>cachedInvokerUrls: stores valid provider URLs</li>
     * </ul>
     *
     * @param invokerUrls List of invoker URLs
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
     * Convert URL list to invoker map.
     * 
     * <p>The conversion process:
     * <ol>
     * <li>Filter URLs by protocol if protocol is specified in queryMap</li>
     * <li>Skip empty protocol URLs</li>
     * <li>Verify protocol is supported</li>
     * <li>Merge URL parameters according to the priority</li>
     * <li>Create invoker from URL if not in cache</li>
     * </ol>
     *
     * <p>Implementation details:
     * <ul>
     * <li>Protocol filtering: Only URLs matching the protocols specified in reference URL are processed</li>
     * <li>Protocol validation: Ensures the protocol is supported by checking extension loader</li>
     * <li>URL deduplication: Uses full URL string as key to avoid duplicate processing</li>
     * <li>Cache mechanism: Reuses existing invokers to avoid unnecessary recreation</li>
     * <li>Invoker state: Checks disabled/enabled state before creating new invokers</li>
     * </ul>
     *
     * <p>Key steps in invoker creation:
     * <ul>
     * <li>URL merging: Combines parameters from multiple sources</li>
     * <li>State check: Verifies if the invoker is enabled</li>
     * <li>Protocol reference: Creates the underlying protocol invoker</li>
     * <li>Delegate wrapping: Wraps protocol invoker with InvokerDelegate for additional metadata</li>
     * </ul>
     *
     * @param urls List of provider URLs
     * @return Map of URL string to invoker
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
     * Route the given invokers using the specified method.
     *
     * <p>For each router:
     * <ul>
     * <li>Skip runtime routers</li>
     * <li>Apply router rules to filter invokers</li>
     * </ul>
     *
     * @param invokers List of invokers to route
     * @param method Method name to route for
     * @return Filtered list of invokers
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
     * Convert invoker map to method invoker map.
     * 
     * <p>The conversion follows these rules:
     * <ol>
     * <li>Extract methods from invoker URL parameters</li>
     * <li>Group invokers by method name</li>
     * <li>Route invokers for each method</li>
     * <li>Add default route for any method</li>
     * <li>Sort and make unmodifiable</li>
     * </ol>
     *
     * <p>Implementation details:
     * <ul>
     * <li>Method extraction: Reads 'methods' parameter from provider URL</li>
     * <li>Method grouping: Creates separate invoker lists for each method</li>
     * <li>Default handling: Maintains a default invoker list for undefined methods</li>
     * <li>Routing: Applies router rules to filter invokers for each method</li>
     * <li>Thread safety: Returns unmodifiable collections to prevent concurrent modification</li>
     * </ul>
     *
     * <p>Special handling:
     * <ul>
     * <li>ANY_VALUE: Default invoker list for methods not explicitly defined</li>
     * <li>Empty methods: Falls back to the default invoker list</li>
     * <li>Method routing: Each method gets its own filtered invoker list</li>
     * <li>Sorting: Invokers are sorted by URL to ensure consistent order</li>
     * </ul>
     *
     * @param invokersMap Map of URL to invoker
     * @return Map of method name to list of invokers
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
     * Close all invokers
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
     * Destroy unused invokers from the old invoker map.
     *
     * <p>This prevents memory leaks when invokers are no longer needed.
     * If refer.autodestroy=false, invokers will only increase without decreasing.
     *
     * @param oldUrlInvokerMap Old invoker map
     * @param newUrlInvokerMap New invoker map
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

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public URL getUrl() {
        return this.overrideDirectoryUrl;
    }

    public URL getRegisteredConsumerUrl() {
        return registeredConsumerUrl;
    }

    public void setRegisteredConsumerUrl(URL registeredConsumerUrl) {
        this.registeredConsumerUrl = registeredConsumerUrl;
    }

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
     * Haomin: added for test purpose
     */
    public Map<String, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<String, List<Invoker<T>>> getMethodInvokerMap() {
        return methodInvokerMap;
    }

    private static class InvokerComparator implements Comparator<Invoker<?>> {

        private static final InvokerComparator comparator = new InvokerComparator();

        private InvokerComparator() {
        }

        public static InvokerComparator getComparator() {
            return comparator;
        }

        @Override
        public int compare(Invoker<?> o1, Invoker<?> o2) {
            return o1.getUrl().toString().compareTo(o2.getUrl().toString());
        }

    }

    /**
     * The delegate class, which is mainly used to store the URL address sent by the registry,and can be reassembled on the basis of providerURL queryMap overrideMap for re-refer.
     *
     * @param <T>
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
