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
package com.alibaba.dubbo.rpc.cluster.router;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock服务路由选择器
 * 
 * 该路由器专门用于处理Mock服务的调用，主要功能：
 * 1. 当请求配置为使用Mock时，只返回协议为MOCK的服务提供者
 * 2. 当请求配置为不使用Mock时，排除所有协议为MOCK的服务提供者
 * 3. 用于实现服务降级和测试场景
 * 
 * 特点：
 * 1. 具有最高优先级，确保Mock路由在其他路由之前执行
 * 2. 支持动态配置，可以在运行时决定是否使用Mock
 * 3. 与其他路由规则相互独立，专注于Mock服务的处理
 */
public class MockInvokersSelector extends AbstractRouter {

    /**
     * 路由器的默认优先级
     * 设置为Integer.MAX_VALUE确保最后执行，这样可以优先处理Mock请求
     */
    private static final int DEFAULT_PRIORITY = Integer.MAX_VALUE;

    /**
     * 构造函数
     * 初始化路由器的优先级为最高优先级
     */
    public MockInvokersSelector() {
        this.priority = DEFAULT_PRIORITY;
    }

    /**
     * 根据Mock配置进行服务路由
     * 
     * @param invokers 原始的服务提供者列表
     * @param url 服务URL
     * @param invocation 调用信息
     * @return 经过Mock路由筛选后的服务提供者列表
     * @throws RpcException 路由过程出现异常
     */
    @Override
    public <T> List<Invoker<T>> route(final List<Invoker<T>> invokers,
                                      URL url, final Invocation invocation) throws RpcException {
        if (invocation.getAttachments() == null) {
            return getNormalInvokers(invokers);
        } else {
            String value = invocation.getAttachments().get(Constants.INVOCATION_NEED_MOCK);
            if (value == null)
                return getNormalInvokers(invokers);
            else if (Boolean.TRUE.toString().equalsIgnoreCase(value)) {
                return getMockedInvokers(invokers);
            }
        }
        return invokers;
    }

    /**
     * 获取Mock服务提供者列表
     * 
     * @param invokers 原始的服务提供者列表
     * @return 仅包含Mock协议的服务提供者列表，如果没有Mock提供者则返回null
     */
    private <T> List<Invoker<T>> getMockedInvokers(final List<Invoker<T>> invokers) {
        if (!hasMockProviders(invokers)) {
            return null;
        }
        List<Invoker<T>> sInvokers = new ArrayList<Invoker<T>>(1);
        for (Invoker<T> invoker : invokers) {
            if (invoker.getUrl().getProtocol().equals(Constants.MOCK_PROTOCOL)) {
                sInvokers.add(invoker);
            }
        }
        return sInvokers;
    }

    /**
     * 获取正常服务提供者列表
     * 
     * @param invokers 原始的服务提供者列表
     * @return 排除了Mock协议的服务提供者列表
     */
    private <T> List<Invoker<T>> getNormalInvokers(final List<Invoker<T>> invokers) {
        if (!hasMockProviders(invokers)) {
            return invokers;
        } else {
            List<Invoker<T>> sInvokers = new ArrayList<Invoker<T>>(invokers.size());
            for (Invoker<T> invoker : invokers) {
                if (!invoker.getUrl().getProtocol().equals(Constants.MOCK_PROTOCOL)) {
                    sInvokers.add(invoker);
                }
            }
            return sInvokers;
        }
    }

    /**
     * 检查是否存在Mock服务提供者
     * 
     * @param invokers 服务提供者列表
     * @return 如果存在Mock协议的提供者返回true，否则返回false
     */
    private <T> boolean hasMockProviders(final List<Invoker<T>> invokers) {
        boolean hasMockProvider = false;
        for (Invoker<T> invoker : invokers) {
            if (invoker.getUrl().getProtocol().equals(Constants.MOCK_PROTOCOL)) {
                hasMockProvider = true;
                break;
            }
        }
        return hasMockProvider;
    }

}
