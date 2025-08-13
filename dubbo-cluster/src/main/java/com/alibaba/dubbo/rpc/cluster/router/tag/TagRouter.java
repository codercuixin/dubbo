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
package com.alibaba.dubbo.rpc.cluster.router.tag;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.router.AbstractRouter;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于标签的路由器
 * 
 * 该路由器用于实现基于标签的服务路由，主要用于以下场景：
 * 1. 服务分组：将服务提供者按标签分组，消费者可以指定调用特定标签的服务
 * 2. 灰度发布：通过标签区分新旧版本，实现灰度发布
 * 3. 环境隔离：通过标签区分不同环境（如测试、预发布等）
 * 
 * 路由规则：
 * 1. 优先调用与消费者具有相同标签的提供者
 * 2. 如果没有找到匹配的标签，且不强制使用标签，则降级为调用无标签的提供者
 * 3. 如果强制使用标签且没有找到匹配的提供者，则返回空列表
 */
public class TagRouter extends AbstractRouter {

    /**
     * 路由器的默认优先级
     */
    private static final int DEFAULT_PRIORITY = 100;

    /**
     * 路由器的默认URL
     * 用于初始化路由器，并设置为运行时动态路由
     */
    private static final URL ROUTER_URL = new URL("tag", Constants.ANYHOST_VALUE, 0, Constants.ANY_VALUE).addParameters(Constants.RUNTIME_KEY, "true");

    /**
     * 构造函数
     * 初始化路由器的URL和优先级
     */
    public TagRouter() {
        this.url = ROUTER_URL;
        this.priority = url.getParameter(Constants.PRIORITY_KEY, DEFAULT_PRIORITY);
    }

    @Override
    public URL getUrl() {
        return url;
    }

    /**
     * 根据标签进行服务路由
     * 
     * @param invokers 原始的服务提供者列表
     * @param url 消费者的URL
     * @param invocation 调用信息
     * @return 经过标签路由筛选后的服务提供者列表
     * @throws RpcException 路由过程出现异常
     */
    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        // filter
        List<Invoker<T>> result = new ArrayList<Invoker<T>>();
        // Dynamic param
        String tag = RpcContext.getContext().getAttachment(Constants.TAG_KEY);
        // Tag request
        if (!StringUtils.isEmpty(tag)) {
            // Select tag invokers first
            for (Invoker<T> invoker : invokers) {
                if (tag.equals(invoker.getUrl().getParameter(Constants.TAG_KEY))) {
                    result.add(invoker);
                }
            }
        }
        // If Constants.REQUEST_TAG_KEY unspecified or no invoker be selected, downgrade to normal invokers
        if (result.isEmpty()) {
            // Only forceTag = true force match, otherwise downgrade
            String forceTag = RpcContext.getContext().getAttachment(Constants.FORCE_USE_TAG);
            if (StringUtils.isEmpty(forceTag) || "false".equals(forceTag)) {
                for (Invoker<T> invoker : invokers) {
                    if (StringUtils.isEmpty(invoker.getUrl().getParameter(Constants.TAG_KEY))) {
                        result.add(invoker);
                    }
                }
            }
        }
        return result;
    }

}
