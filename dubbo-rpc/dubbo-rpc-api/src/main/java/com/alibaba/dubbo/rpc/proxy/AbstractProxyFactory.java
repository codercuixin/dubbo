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
package com.alibaba.dubbo.rpc.proxy;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.service.EchoService;
import com.alibaba.dubbo.rpc.service.GenericService;

/**
 * 代理工厂抽象类
 * 
 * 该类实现了ProxyFactory接口，为Dubbo提供代理实例的创建功能。
 * 主要职责：
 * 1. 创建服务代理实例
 * 2. 处理泛化调用代理
 * 3. 支持回声测试服务（EchoService）
 */
public abstract class AbstractProxyFactory implements ProxyFactory {

    /**
     * 获取代理实例（不支持泛化调用）
     * 
     * @param invoker 调用者对象
     * @return 代理实例
     * @throws RpcException RPC异常
     */
    @Override
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        return getProxy(invoker, false);
    }

    /**
     * 获取代理实例
     * 
     * @param invoker 调用者对象
     * @param generic 是否支持泛化调用
     * @return 代理实例
     * @throws RpcException RPC异常
     */
    @Override
    public <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException {
        Class<?>[] interfaces = null;
        // 从URL中获取接口配置
        String config = invoker.getUrl().getParameter("interfaces");
        if (config != null && config.length() > 0) {
            // 解析配置的接口列表
            String[] types = Constants.COMMA_SPLIT_PATTERN.split(config);
            if (types != null && types.length > 0) {
                // 创建接口数组，包含原始接口和EchoService
                interfaces = new Class<?>[types.length + 2];
                interfaces[0] = invoker.getInterface();
                interfaces[1] = EchoService.class;
                // 加载配置的接口类
                for (int i = 0; i < types.length; i++) {
                    interfaces[i + 2] = ReflectUtils.forName(types[i]);
                }
            }
        }
        // 如果没有配置额外接口，则使用默认接口数组
        if (interfaces == null) {
            interfaces = new Class<?>[]{invoker.getInterface(), EchoService.class};
        }

        // 处理泛化调用支持
        if (!invoker.getInterface().equals(GenericService.class) && generic) {
            int len = interfaces.length;
            Class<?>[] temp = interfaces;
            interfaces = new Class<?>[len + 1];
            System.arraycopy(temp, 0, interfaces, 0, len);
            interfaces[len] = GenericService.class;
        }

        return getProxy(invoker, interfaces);
    }

    /**
     * 根据调用者和接口列表创建代理实例（抽象方法）
     * 
     * @param invoker 调用者对象
     * @param types 接口类型数组
     * @return 代理实例
     */
    public abstract <T> T getProxy(Invoker<T> invoker, Class<?>[] types);

}
