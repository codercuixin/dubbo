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
package com.alibaba.dubbo.rpc.cluster.router.script;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.router.AbstractRouter;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于脚本的路由器
 * 
 * 该路由器允许用户通过脚本语言定义路由规则，具有以下特点：
 * 1. 支持多种脚本语言（如JavaScript、Groovy等）
 * 2. 提供更灵活的路由规则定义方式
 * 3. 支持动态编译和缓存脚本
 * 4. 实现了基本的安全控制
 * 
 * 使用方式：
 * 1. 在URL中通过type参数指定脚本语言类型
 * 2. 通过rule参数提供具体的路由规则脚本
 * 3. 脚本中可以访问：invokers列表、invocation对象、context上下文
 * 
 * 安全说明：
 * 1. 使用AccessControlContext限制脚本的权限
 * 2. 只允许访问必要的反射权限
 * 3. 建议在信任的环境中使用脚本路由
 */
public class ScriptRouter extends AbstractRouter {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(ScriptRouter.class);

    /**
     * 路由器的默认优先级
     */
    private static final int DEFAULT_PRIORITY = 1;

    /**
     * 脚本引擎缓存
     * key: 脚本类型（如js、groovy等）
     * value: 对应的脚本引擎实例
     */
    private static final Map<String, ScriptEngine> engines = new ConcurrentHashMap<String, ScriptEngine>();

    /**
     * 当前路由器使用的脚本引擎实例
     */
    private final ScriptEngine engine;

    /**
     * 路由规则脚本内容
     */
    private final String rule;

    /**
     * 编译后的脚本函数
     * 使用CompiledScript提高执行效率
     */
    private CompiledScript function;

    /**
     * 安全上下文
     * 用于限制脚本的执行权限
     */
    private AccessControlContext accessControlContext;

    {
        //Just give permission of reflect to access member.
        Permissions perms = new Permissions();
        perms.add(new RuntimePermission("accessDeclaredMembers"));
        // Cast to Certificate[] required because of ambiguity:
        ProtectionDomain domain = new ProtectionDomain(new CodeSource(null, (Certificate[]) null), perms);
        accessControlContext = new AccessControlContext(new ProtectionDomain[]{domain});
    }

    /**
     * 构造函数
     * 
     * @param url 包含路由配置的URL，必须包含：
     *            - type: 脚本语言类型
     *            - rule: 路由规则脚本
     *            - priority: 可选，路由器优先级
     */
    public ScriptRouter(URL url) {
        this.url = url;
        this.priority = url.getParameter(Constants.PRIORITY_KEY, DEFAULT_PRIORITY);

        // 初始化脚本引擎和规则
        this.engine = getEngine(url);
        this.rule = getRule(url);

        // 编译脚本以提高执行效率
        try {
            Compilable compilable = (Compilable) engine;
            function = compilable.compile(rule);
        } catch (ScriptException e) {
            logger.error("route error, rule has been ignored. rule: " + rule +
                    ", url: " + RpcContext.getContext().getUrl(), e);
        }
    }

    /**
     * 从URL参数中获取路由规则脚本
     * 
     * @param url 包含路由配置的URL
     * @return 路由规则脚本内容
     * @throws IllegalStateException 当规则为空时抛出异常
     */
    private String getRule(URL url) {
        String vRule = url.getParameterAndDecoded(Constants.RULE_KEY);
        if (StringUtils.isEmpty(vRule)) {
            throw new IllegalStateException("route rule can not be empty.");
        }
        return vRule;
    }

    /**
     * 根据URL中指定的类型创建或获取脚本引擎实例
     * 使用缓存机制避免重复创建相同类型的脚本引擎
     * 
     * @param url 包含脚本类型配置的URL
     * @return 脚本引擎实例
     * @throws IllegalStateException 当指定的脚本类型不支持时抛出异常
     */
    private ScriptEngine getEngine(URL url) {
        String type = url.getParameter(Constants.TYPE_KEY, Constants.DEFAULT_SCRIPT_TYPE_KEY);
        ScriptEngine engine = engines.get(type);
        if (engine == null) {
            engine = new ScriptEngineManager().getEngineByName(type);
            if (engine == null) {
                throw new IllegalStateException(new IllegalStateException("Unsupported route rule type: " + type + ", rule: " + rule));
            }
            engines.put(type, engine);
        }

        return engine;
    }


    /**
     * 执行路由规则脚本进行服务路由
     * 
     * 路由过程：
     * 1. 创建脚本执行的上下文（bindings）
     * 2. 在受限的权限环境下执行脚本
     * 3. 处理脚本的返回结果
     * 
     * @param invokers 原始的服务提供者列表
     * @param url 服务URL
     * @param invocation 调用信息
     * @return 经过路由筛选后的服务提供者列表
     * @throws RpcException 路由过程出现异常
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<Invoker<T>> route(final List<Invoker<T>> invokers, URL url, final Invocation invocation) throws RpcException {
        if (engine == null || function == null) {
            return invokers;
        }
        final Bindings bindings = createBindings(invokers, invocation);
        return getRoutedInvokers(AccessController.doPrivileged(new PrivilegedAction() {
            @Override
            public Object run() {
                try {
                    return function.eval(bindings);
                } catch (ScriptException e) {
                    logger.error("route error, rule has been ignored. rule: " + rule + ", method:" +
                            invocation.getMethodName() + ", url: " + RpcContext.getContext().getUrl(), e);
                    return invokers;
                }
            }
        }, accessControlContext));
    }

    /**
     * 处理脚本执行的返回结果，将其转换为Invoker列表
     * 
     * 支持以下返回类型：
     * 1. Invoker数组
     * 2. Object数组（元素为Invoker）
     * 3. Invoker列表
     * 
     * @param obj 脚本执行的返回结果
     * @return 转换后的Invoker列表
     */
    @SuppressWarnings("unchecked")
    protected <T> List<Invoker<T>> getRoutedInvokers(Object obj) {
        if (obj instanceof Invoker[]) {
            return Arrays.asList((Invoker<T>[]) obj);
        } else if (obj instanceof Object[]) {
            Object[] objects = (Object[]) obj;
            List<Invoker<T>> invokers = new ArrayList<Invoker<T>>();
            for (Object object : objects) {
                invokers.add((Invoker<T>) object);
            }

            return invokers;
        } else {
            return (List<Invoker<T>>) obj;
        }
    }

    /**
     * 创建脚本执行的上下文环境
     * 
     * 向脚本提供以下变量：
     * 1. invokers: 服务提供者列表
     * 2. invocation: 调用信息
     * 3. context: RPC上下文
     * 
     * @param invokers 服务提供者列表
     * @param invocation 调用信息
     * @return 包含必要变量的脚本上下文
     */
    private <T> Bindings createBindings(List<Invoker<T>> invokers, Invocation invocation) {
        Bindings bindings = engine.createBindings();
        // create a new List of invokers
        bindings.put("invokers", new ArrayList<Invoker<T>>(invokers));
        bindings.put("invocation", invocation);
        bindings.put("context", RpcContext.getContext());
        return bindings;
    }
}
