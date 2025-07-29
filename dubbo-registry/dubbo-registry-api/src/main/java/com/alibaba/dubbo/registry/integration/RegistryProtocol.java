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
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryFactory;
import com.alibaba.dubbo.registry.RegistryService;
import com.alibaba.dubbo.registry.support.ProviderConsumerRegTable;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.Configurator;
import com.alibaba.dubbo.rpc.protocol.InvokerWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alibaba.dubbo.common.Constants.ACCEPT_FOREIGN_IP;
import static com.alibaba.dubbo.common.Constants.QOS_ENABLE;
import static com.alibaba.dubbo.common.Constants.QOS_PORT;
import static com.alibaba.dubbo.common.Constants.VALIDATION_KEY;
import static com.alibaba.dubbo.common.Constants.CATEGORY_KEY;
import static com.alibaba.dubbo.common.Constants.CONSUMERS_CATEGORY;
import static com.alibaba.dubbo.common.Constants.CHECK_KEY;

/**
 * 注册中心协议实现类
 * 
 * RegistryProtocol是Dubbo的核心协议之一，负责服务注册与发现的核心逻辑。
 * 它实现了Protocol接口，主要功能包括：
 * 1. 服务导出：将本地服务注册到注册中心，并监听配置变更
 * 2. 服务引用：从注册中心获取服务提供者列表，创建集群Invoker
 * 3. 服务治理：支持动态配置、路由规则、负载均衡等
 * 4. 生命周期管理：管理服务的导出和销毁
 * 
 * 该类是Dubbo服务治理的核心组件，协调了注册中心、集群、协议等多个模块。
 */
public class RegistryProtocol implements Protocol {

    /** 日志记录器 */
    private final static Logger logger = LoggerFactory.getLogger(RegistryProtocol.class);
    
    /** RegistryProtocol单例实例 */
    private static RegistryProtocol INSTANCE;
    
    /** 配置覆盖监听器映射表，用于监听服务配置的动态变更 */
    private final Map<URL, NotifyListener> overrideListeners = new ConcurrentHashMap<URL, NotifyListener>();
    
    /** 
     * 已导出服务的缓存映射表
     * 为了解决RMI重复暴露端口冲突问题，已经暴露的服务不再重复暴露
     * key: providerUrl <--> value: exporter包装器
     */
    private final Map<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<String, ExporterChangeableWrapper<?>>();
    
    /** 集群策略，用于处理多个服务提供者的调用逻辑 */
    private Cluster cluster;
    
    /** 具体的协议实现，用于创建远程调用的Invoker */
    private Protocol protocol;
    
    /** 注册中心工厂，用于创建注册中心实例 */
    private RegistryFactory registryFactory;
    
    /** 代理工厂，用于创建服务代理 */
    private ProxyFactory proxyFactory;

    /**
     * 构造函数，设置单例实例
     */
    public RegistryProtocol() {
        INSTANCE = this;
    }

    /**
     * 获取RegistryProtocol单例实例
     * 
     * 如果实例不存在，会通过SPI机制加载RegistryProtocol实例
     * 
     * @return RegistryProtocol单例实例
     */
    public static RegistryProtocol getRegistryProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(Constants.REGISTRY_PROTOCOL); // load
        }
        return INSTANCE;
    }

    /**
     * 过滤URL中不需要输出的参数（以.开头的参数）
     * 
     * 这些参数通常是内部使用的配置参数，不应该暴露给外部
     * 
     * @param url 需要过滤参数的URL
     * @return 需要过滤的参数键数组
     */
    private static String[] getFilteredKeys(URL url) {
        Map<String, String> params = url.getParameters();
        if (params != null && !params.isEmpty()) {
            List<String> filteredKeys = new ArrayList<String>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (entry != null && entry.getKey() != null && entry.getKey().startsWith(Constants.HIDE_KEY_PREFIX)) {
                    filteredKeys.add(entry.getKey());
                }
            }
            return filteredKeys.toArray(new String[filteredKeys.size()]);
        } else {
            return new String[]{};
        }
    }

    /**
     * 设置集群策略
     * 
     * @param cluster 集群策略实例
     */
    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * 设置协议实现
     * 
     * @param protocol 协议实现实例
     */
    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    /**
     * 设置注册中心工厂
     * 
     * @param registryFactory 注册中心工厂实例
     */
    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    /**
     * 设置代理工厂
     * 
     * @param proxyFactory 代理工厂实例
     */
    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    /**
     * 获取默认端口号
     * 
     * @return 默认端口号9090
     */
    @Override
    public int getDefaultPort() {
        return 9090;
    }

    /**
     * 获取配置覆盖监听器映射表
     * 
     * @return 配置覆盖监听器映射表
     */
    public Map<URL, NotifyListener> getOverrideListeners() {
        return overrideListeners;
    }

    /**
     * 注册服务提供者到注册中心
     * 
     * @param registryUrl 注册中心URL
     * @param registedProviderUrl 要注册的服务提供者URL
     */
    public void register(URL registryUrl, URL registedProviderUrl) {
        Registry registry = registryFactory.getRegistry(registryUrl);
        registry.register(registedProviderUrl);
    }

    /**
     * 导出服务，将本地服务注册到注册中心
     * 
     * 该方法实现了Dubbo的服务导出机制，主要功能包括：
     * 1. 本地导出：创建本地Exporter，启动服务监听
     * 2. 服务注册：将服务信息注册到注册中心
     * 3. 配置订阅：订阅配置中心的动态配置变更
     * 4. 生命周期管理：返回可销毁的Exporter包装器
     * 
     * @param originInvoker 原始的服务Invoker
     * @return 返回可销毁的Exporter包装器
     * @throws RpcException 当服务导出失败时抛出RPC异常
     */
    @Override
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
        // 本地导出服务，创建Exporter包装器
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker);

        // 获取注册中心URL
        URL registryUrl = getRegistryUrl(originInvoker);

        // 获取注册中心实例和注册的服务提供者URL
        final Registry registry = getRegistry(originInvoker);
        final URL registeredProviderUrl = getRegisteredProviderUrl(originInvoker);

        // 判断是否需要注册到注册中心（支持延迟发布）
        boolean register = registeredProviderUrl.getParameter("register", true);

        // 注册服务提供者信息到ProviderConsumerRegTable
        ProviderConsumerRegTable.registerProvider(originInvoker, registryUrl, registeredProviderUrl);

        // 如果需要注册，则注册到注册中心
        if (register) {
            register(registryUrl, registeredProviderUrl);
            ProviderConsumerRegTable.getProviderWrapper(originInvoker).setReg(true);
        }

        // 订阅配置覆盖数据
        // FIXME: 当提供者订阅时，会影响场景：某个JVM暴露服务并调用相同服务。
        // 因为订阅的缓存键是服务名称，导致订阅信息被覆盖
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registeredProviderUrl);
        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl, originInvoker);
        overrideListeners.put(overrideSubscribeUrl, overrideSubscribeListener);
        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);
        
        // 确保每次导出都返回新的exporter实例
        return new DestroyableExporter<T>(exporter, originInvoker, overrideSubscribeUrl, registeredProviderUrl);
    }

    /**
     * 执行本地服务导出
     * 
     * 该方法负责在本地启动服务监听，并缓存Exporter以避免重复导出。
     * 使用双重检查锁定模式确保线程安全。
     * 
     * @param originInvoker 原始的服务Invoker
     * @return 返回Exporter包装器，支持动态修改
     */
    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker) {
        String key = getCacheKey(originInvoker);
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            synchronized (bounds) {
                exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
                if (exporter == null) {
                    // 创建代理Invoker，使用实际的提供者URL
                    final Invoker<?> invokerDelegete = new InvokerDelegete<T>(originInvoker, getProviderUrl(originInvoker));
                    // 使用具体协议导出服务，并包装为可修改的Exporter
                    exporter = new ExporterChangeableWrapper<T>((Exporter<T>) protocol.export(invokerDelegete), originInvoker);
                    bounds.put(key, exporter);
                }
            }
        }
        return exporter;
    }

    /**
     * 重新导出修改后的URL对应的Invoker
     * 
     * 当服务配置发生动态变更时，需要重新导出服务以应用新的配置。
     * 该方法会创建新的代理Invoker并更新Exporter。
     *
     * @param originInvoker 原始的服务Invoker
     * @param newInvokerUrl 新的服务URL
     */
    @SuppressWarnings("unchecked")
    private <T> void doChangeLocalExport(final Invoker<T> originInvoker, URL newInvokerUrl) {
        String key = getCacheKey(originInvoker);
        final ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            logger.warn(new IllegalStateException("error state, exporter should not be null"));
        } else {
            // 创建新的代理Invoker，使用新的URL
            final Invoker<T> invokerDelegete = new InvokerDelegete<T>(originInvoker, newInvokerUrl);
            // 重新导出服务并更新Exporter
            exporter.setExporter(protocol.export(invokerDelegete));
        }
    }

    /**
     * 根据Invoker的地址获取注册中心实例
     *
     * @param originInvoker 原始的服务Invoker
     * @return 返回注册中心实例
     */
    private Registry getRegistry(final Invoker<?> originInvoker) {
        URL registryUrl = getRegistryUrl(originInvoker);
        return registryFactory.getRegistry(registryUrl);
    }

    /**
     * 获取注册中心URL
     * 
     * 如果URL的协议是registry，则解析出实际的注册中心协议（如zookeeper）
     * 
     * @param originInvoker 原始的服务Invoker
     * @return 返回注册中心URL
     */
    private URL getRegistryUrl(Invoker<?> originInvoker) {
        URL registryUrl = originInvoker.getUrl();
        if (Constants.REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            String protocol = registryUrl.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_DIRECTORY);
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(Constants.REGISTRY_KEY);
        }
        return registryUrl;
    }


    /**
     * 获取注册到注册中心的URL，并过滤不需要的参数
     * 
     * 该方法会移除一些内部参数，只保留需要暴露给注册中心的参数
     *
     * @param originInvoker 原始的服务Invoker
     * @return 返回过滤后的提供者URL
     */
    private URL getRegisteredProviderUrl(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        // 移除不需要暴露给注册中心的参数
        return providerUrl.removeParameters(getFilteredKeys(providerUrl))
                .removeParameter(Constants.MONITOR_KEY)
                .removeParameter(Constants.BIND_IP_KEY)
                .removeParameter(Constants.BIND_PORT_KEY)
                .removeParameter(QOS_ENABLE)
                .removeParameter(QOS_PORT)
                .removeParameter(ACCEPT_FOREIGN_IP)
                .removeParameter(VALIDATION_KEY);
    }

    /**
     * 获取订阅配置覆盖的URL
     * 
     * 用于订阅配置中心的动态配置变更
     * 
     * @param registedProviderUrl 已注册的提供者URL
     * @return 返回订阅配置覆盖的URL
     */
    private URL getSubscribedOverrideUrl(URL registedProviderUrl) {
        return registedProviderUrl.setProtocol(Constants.PROVIDER_PROTOCOL)
                .addParameters(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY,
                        Constants.CHECK_KEY, String.valueOf(false));
    }

    /**
     * 通过Invoker的URL获取提供者URL
     *
     * @param origininvoker 原始的服务Invoker
     * @return 返回提供者URL
     * @throws IllegalArgumentException 当export参数为空时抛出异常
     */
    private URL getProviderUrl(final Invoker<?> origininvoker) {
        String export = origininvoker.getUrl().getParameterAndDecoded(Constants.EXPORT_KEY);
        if (export == null || export.length() == 0) {
            throw new IllegalArgumentException("The registry export url is null! registry: " + origininvoker.getUrl());
        }

        URL providerUrl = URL.valueOf(export);
        return providerUrl;
    }

    /**
     * 获取Invoker在bounds缓存中的键
     *
     * @param originInvoker 原始的服务Invoker
     * @return 返回缓存键
     */
    private String getCacheKey(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        String key = providerUrl.removeParameters("dynamic", "enabled").toFullString();
        return key;
    }

    /**
     * 引用远程服务，创建服务消费者的Invoker
     * 
     * 该方法实现了Dubbo的服务发现和引用机制，主要功能包括：
     * 1. 从注册中心获取服务提供者列表
     * 2. 根据分组策略选择合适的集群策略
     * 3. 创建RegistryDirectory进行服务目录管理
     * 4. 订阅服务变更通知
     * 5. 返回可调用的Invoker对象
     * 
     * @param type 服务接口类型
     * @param url 服务引用URL，包含注册中心地址、服务接口、分组等信息
     * @return 返回服务消费者的Invoker，用于远程调用
     * @throws RpcException 当服务引用失败时抛出RPC异常
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        // 解析注册中心协议，默认为zookeeper，并移除registry参数
        url = url.setProtocol(url.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_REGISTRY)).removeParameter(Constants.REGISTRY_KEY);
        // 获取注册中心实例
        Registry registry = registryFactory.getRegistry(url);
        
        // 如果引用的是注册中心服务本身，直接返回注册中心的代理Invoker
        if (RegistryService.class.equals(type)) {
            return proxyFactory.getInvoker((T) registry, type, url);
        }

        // 解析引用参数，检查分组配置
        // 支持多种分组格式：group="a,b" 或 group="*"
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        String group = qs.get(Constants.GROUP_KEY);
        if (group != null && group.length() > 0) {
            // 如果分组包含多个值或为通配符"*"，使用可合并集群策略
            if ((Constants.COMMA_SPLIT_PATTERN.split(group)).length > 1
                    || "*".equals(group)) {
                return doRefer(getMergeableCluster(), registry, type, url);
            }
        }
        // 使用默认集群策略进行服务引用
        return doRefer(cluster, registry, type, url);
    }

    /**
     * 获取可合并集群策略
     * 
     * 用于处理多分组场景下的服务合并
     * 
     * @return 返回可合并集群策略实例
     */
    private Cluster getMergeableCluster() {
        return ExtensionLoader.getExtensionLoader(Cluster.class).getExtension("mergeable");
    }

    /**
     * 执行具体的服务引用逻辑
     * 
     * 该方法负责创建服务目录、注册消费者、订阅服务变更，并最终返回集群Invoker。
     * 主要步骤包括：
     * 1. 创建RegistryDirectory服务目录，用于管理服务提供者列表
     * 2. 设置注册中心和协议实例
     * 3. 构建订阅URL，用于监听服务变更
     * 4. 注册消费者信息到注册中心（可选）
     * 5. 订阅服务提供者、配置中心和路由器的变更通知
     * 6. 使用集群策略包装目录，返回可调用的Invoker
     * 7. 注册消费者信息到ProviderConsumerRegTable
     * 
     * @param cluster 集群策略，用于处理多个服务提供者的调用逻辑
     * @param registry 注册中心实例，用于服务发现和注册
     * @param type 服务接口类型
     * @param url 服务引用URL，包含服务接口、分组、版本等信息
     * @return 返回集群Invoker，封装了负载均衡、容错等集群策略
     */
    private <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type, URL url) {
        // 创建服务目录，用于管理服务提供者列表和路由规则
        RegistryDirectory<T> directory = new RegistryDirectory<T>(type, url);
        // 设置注册中心实例
        directory.setRegistry(registry);
        // 设置协议实例，用于创建具体的远程调用Invoker
        directory.setProtocol(protocol);
        
        // 获取所有引用参数，用于构建订阅URL
        Map<String, String> parameters = new HashMap<String, String>(directory.getUrl().getParameters());
        // 构建订阅URL，移除注册IP参数，使用消费者协议
        URL subscribeUrl = new URL(Constants.CONSUMER_PROTOCOL, parameters.remove(Constants.REGISTER_IP_KEY), 0, type.getName(), parameters);
        
        // 如果不是通配符服务接口且需要注册消费者，则注册消费者信息到注册中心
        if (!Constants.ANY_VALUE.equals(url.getServiceInterface())
                && url.getParameter(Constants.REGISTER_KEY, true)) {
            URL registeredConsumerUrl = getRegisteredConsumerUrl(subscribeUrl, url);
            registry.register(registeredConsumerUrl);
            directory.setRegisteredConsumerUrl(registeredConsumerUrl);
        }
        
        // 订阅服务提供者、配置中心和路由器的变更通知
        // 当这些信息发生变化时，RegistryDirectory会收到通知并更新本地缓存
        directory.subscribe(subscribeUrl.addParameter(Constants.CATEGORY_KEY,
                Constants.PROVIDERS_CATEGORY
                        + "," + Constants.CONFIGURATORS_CATEGORY
                        + "," + Constants.ROUTERS_CATEGORY));

        // 使用集群策略包装目录，实现负载均衡、容错等功能
        Invoker invoker = cluster.join(directory);
        // 注册消费者信息到ProviderConsumerRegTable，用于统计和管理
        ProviderConsumerRegTable.registerConsumer(invoker, url, subscribeUrl, directory);
        return invoker;
    }

    /**
     * 获取注册到注册中心的消费者URL
     * 
     * @param consumerUrl 消费者URL
     * @param registryUrl 注册中心URL
     * @return 返回注册用的消费者URL
     */
    public URL getRegisteredConsumerUrl(final URL consumerUrl, URL registryUrl) {
        return consumerUrl.addParameters(CATEGORY_KEY, CONSUMERS_CATEGORY,
                CHECK_KEY, String.valueOf(false));
    }

    /**
     * 销毁RegistryProtocol
     * 
     * 清理所有已导出的服务，释放资源
     */
    @Override
    public void destroy() {
        List<Exporter<?>> exporters = new ArrayList<Exporter<?>>(bounds.values());
        for (Exporter<?> exporter : exporters) {
            exporter.unexport();
        }
        bounds.clear();
    }

    /**
     * Invoker代理类
     * 
     * 用于包装原始Invoker，提供URL重写功能
     * 
     * @param <T> 服务类型
     */
    public static class InvokerDelegete<T> extends InvokerWrapper<T> {
        private final Invoker<T> invoker;

        /**
         * 构造函数
         * 
         * @param invoker 原始Invoker
         * @param url 新的URL，invoker.getUrl()将返回这个值
         */
        public InvokerDelegete(Invoker<T> invoker, URL url) {
            super(invoker, url);
            this.invoker = invoker;
        }

        /**
         * 获取原始Invoker
         * 
         * 如果当前Invoker是代理，则递归获取原始Invoker
         * 
         * @return 返回原始Invoker
         */
        public Invoker<T> getInvoker() {
            if (invoker instanceof InvokerDelegete) {
                return ((InvokerDelegete<T>) invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }

    /**
     * 配置覆盖监听器
     * 
     * 用于监听配置中心的动态配置变更，并重新导出服务以应用新配置。
     * 
     * 重新导出：解决协议中exporter销毁的问题
     * 1. 确保RegistryProtocol返回的exporter能够正常销毁
     * 2. 通知后无需重新注册到注册中心
     * 3. export方法传递的invoker最好是exporter的invoker
     */
    private class OverrideListener implements NotifyListener {

        /** 订阅URL */
        private final URL subscribeUrl;
        /** 原始Invoker */
        private final Invoker originInvoker;

        /**
         * 构造函数
         * 
         * @param subscribeUrl 订阅URL
         * @param originalInvoker 原始Invoker
         */
        public OverrideListener(URL subscribeUrl, Invoker originalInvoker) {
            this.subscribeUrl = subscribeUrl;
            this.originInvoker = originalInvoker;
        }

        /**
         * @param urls The list of registered information , is always not empty, The meaning is the same as the return value of {@link com.alibaba.dubbo.registry.RegistryService#lookup(URL)}.
         */
        @Override
        public synchronized void notify(List<URL> urls) {
            logger.debug("original override urls: " + urls);
            List<URL> matchedUrls = getMatchedUrls(urls, subscribeUrl);
            logger.debug("subscribe url: " + subscribeUrl + ", override urls: " + matchedUrls);
            // No matching results
            if (matchedUrls.isEmpty()) {
                return;
            }

            List<Configurator> configurators = RegistryDirectory.toConfigurators(matchedUrls);

            final Invoker<?> invoker;
            if (originInvoker instanceof InvokerDelegete) {
                invoker = ((InvokerDelegete<?>) originInvoker).getInvoker();
            } else {
                invoker = originInvoker;
            }
            //The origin invoker
            URL originUrl = RegistryProtocol.this.getProviderUrl(invoker);
            String key = getCacheKey(originInvoker);
            ExporterChangeableWrapper<?> exporter = bounds.get(key);
            if (exporter == null) {
                logger.warn(new IllegalStateException("error state, exporter should not be null"));
                return;
            }
            //The current, may have been merged many times
            URL currentUrl = exporter.getInvoker().getUrl();
            //Merged with this configuration
            URL newUrl = getConfigedInvokerUrl(configurators, originUrl);
            if (!currentUrl.equals(newUrl)) {
                RegistryProtocol.this.doChangeLocalExport(originInvoker, newUrl);
                logger.info("exported provider url changed, origin url: " + originUrl + ", old export url: " + currentUrl + ", new export url: " + newUrl);
            }
        }

        private List<URL> getMatchedUrls(List<URL> configuratorUrls, URL currentSubscribe) {
            List<URL> result = new ArrayList<URL>();
            for (URL url : configuratorUrls) {
                URL overrideUrl = url;
                // Compatible with the old version
                if (url.getParameter(Constants.CATEGORY_KEY) == null
                        && Constants.OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    overrideUrl = url.addParameter(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY);
                }

                // Check whether url is to be applied to the current service
                if (UrlUtils.isMatch(currentSubscribe, overrideUrl)) {
                    result.add(url);
                }
            }
            return result;
        }

        //Merge the urls of configurators
        private URL getConfigedInvokerUrl(List<Configurator> configurators, URL url) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
            return url;
        }
    }

    /**
     * Exporter代理包装器
     * 
     * 建立返回的exporter与协议导出的exporter之间的对应关系，
     * 并可以在配置覆盖时修改这种关系。
     *
     * @param <T> 服务类型
     */
    private class ExporterChangeableWrapper<T> implements Exporter<T> {

        /** 原始Invoker */
        private final Invoker<T> originInvoker;
        /** 实际的Exporter */
        private Exporter<T> exporter;

        /**
         * 构造函数
         * 
         * @param exporter 实际的Exporter
         * @param originInvoker 原始Invoker
         */
        public ExporterChangeableWrapper(Exporter<T> exporter, Invoker<T> originInvoker) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
        }

        /**
         * 获取原始Invoker
         * 
         * @return 返回原始Invoker
         */
        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        /**
         * 设置Exporter
         * 
         * @param exporter 新的Exporter
         */
        public void setExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        @Override
        public void unexport() {
            String key = getCacheKey(this.originInvoker);
            bounds.remove(key);
            exporter.unexport();
        }
    }

    /**
     * 可销毁的Exporter包装器
     * 
     * 负责在服务销毁时清理注册中心信息和订阅信息
     * 
     * @param <T> 服务类型
     */
    static private class DestroyableExporter<T> implements Exporter<T> {

        /** 用于异步销毁的线程池 */
        public static final ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("Exporter-Unexport", true));

        /** 实际的Exporter */
        private Exporter<T> exporter;
        /** 原始Invoker */
        private Invoker<T> originInvoker;
        /** 订阅URL */
        private URL subscribeUrl;
        /** 注册URL */
        private URL registerUrl;

        /**
         * 构造函数
         * 
         * @param exporter 实际的Exporter
         * @param originInvoker 原始Invoker
         * @param subscribeUrl 订阅URL
         * @param registerUrl 注册URL
         */
        public DestroyableExporter(Exporter<T> exporter, Invoker<T> originInvoker, URL subscribeUrl, URL registerUrl) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
            this.subscribeUrl = subscribeUrl;
            this.registerUrl = registerUrl;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        /**
         * 销毁Exporter
         * 
         * 1. 从注册中心注销服务
         * 2. 取消配置订阅
         * 3. 异步销毁实际的Exporter
         */
        @Override
        public void unexport() {
            Registry registry = RegistryProtocol.INSTANCE.getRegistry(originInvoker);
            try {
                // 从注册中心注销服务
                registry.unregister(registerUrl);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
            try {
                // 取消配置订阅
                NotifyListener listener = RegistryProtocol.INSTANCE.overrideListeners.remove(subscribeUrl);
                registry.unsubscribe(subscribeUrl, listener);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }

            // 异步销毁实际的Exporter
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int timeout = ConfigUtils.getServerShutdownTimeout();
                        if (timeout > 0) {
                            logger.info("Waiting " + timeout + "ms for registry to notify all consumers before unexport. Usually, this is called when you use dubbo API");
                            Thread.sleep(timeout);
                        }
                        exporter.unexport();
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            });
        }
    }
}
