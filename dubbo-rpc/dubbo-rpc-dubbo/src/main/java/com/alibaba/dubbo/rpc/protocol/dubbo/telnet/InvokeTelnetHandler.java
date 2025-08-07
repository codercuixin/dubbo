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
package com.alibaba.dubbo.rpc.protocol.dubbo.telnet;

import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.common.utils.PojoUtils;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.telnet.TelnetHandler;
import com.alibaba.dubbo.remoting.telnet.support.Help;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.protocol.dubbo.DubboProtocol;
import com.alibaba.fastjson.JSON;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Telnet 命令处理器，用于通过命令行调用服务方法
 * 
 * 支持的调用格式：
 * 1. invoke xxxMethod(args) - 调用默认服务的方法
 * 2. invoke XxxService.xxxMethod(args) - 调用指定服务的方法
 * 3. invoke com.xxx.XxxService.xxxMethod(args) - 调用完全限定名的服务方法
 * 
 * 参数格式：
 * - 基本类型：直接传值，如 invoke method(1234)
 * - 字符串：需要用引号，如 invoke method("abcd")
 * - 对象：使用 JSON 格式，如 invoke method({"name": "tom", "age": 20})
 * 
 * 特性：
 * 1. 支持方法重载，会自动匹配最合适的方法
 * 2. 支持参数类型自动转换
 * 3. 支持返回值 JSON 格式化输出
 * 4. 显示方法执行耗时
 */
@Activate
@Help(parameter = "[service.]method(args)", summary = "Invoke the service method.", detail = "Invoke the service method.")
public class InvokeTelnetHandler implements TelnetHandler {
    /**
     * 用于在 Channel 中存储调用消息的键
     * 当存在方法重载时，用于保存用户输入的原始调用命令
     */
    static final String INVOKE_MESSAGE_KEY = "telnet.invoke.method.message";

    /**
     * 用于在 Channel 中存储方法列表的键
     * 当存在方法重载时，用于保存匹配的方法列表
     */
    static final String INVOKE_METHOD_LIST_KEY = "telnet.invoke.method.list";

    /**
     * 处理 telnet 调用命令
     * 
     * @param channel 当前的 telnet 通道
     * @param message 用户输入的调用命令，格式为：[service.]method(args)
     * @return 调用结果，包括：
     *         - 执行成功：返回值的 JSON 字符串 + 执行耗时
     *         - 参数错误：错误提示信息
     *         - 方法不存在：错误提示信息
     *         - 执行异常：异常信息
     * @throws RemotingException 当远程调用发生异常时抛出
     */
    @Override
    public String telnet(Channel channel, String message) {
        // 检查命令是否为空
        if (message == null || message.length() == 0) {
            return "Please input method name, eg: \r\ninvoke xxxMethod(1234, \"abcd\", {\"prop\" : \"value\"})\r\ninvoke XxxService.xxxMethod(1234, \"abcd\", {\"prop\" : \"value\"})\r\ninvoke com.xxx.XxxService.xxxMethod(1234, \"abcd\", {\"prop\" : \"value\"})";
        }

        StringBuilder buf = new StringBuilder();
        
        // 获取默认服务名（如果之前使用了 cd 命令切换到某个服务）
        String service = (String) channel.getAttribute(ChangeTelnetHandler.SERVICE_KEY);
        if (service != null && service.length() > 0) {
            buf.append("Use default service " + service + ".\r\n");
        }

        // 检查命令格式是否正确（是否包含括号）
        int i = message.indexOf("(");
        if (i < 0 || !message.endsWith(")")) {
            return "Invalid parameters, format: service.method(args)";
        }

        // 解析方法名和参数
        String method = message.substring(0, i).trim();
        String args = message.substring(i + 1, message.length() - 1).trim();
        
        // 如果方法名中包含点号，则解析服务名和方法名
        i = method.lastIndexOf(".");
        if (i >= 0) {
            service = method.substring(0, i).trim();
            method = method.substring(i + 1).trim();
        }

        // 将参数字符串解析为 JSON 数组
        List<Object> list;
        try {
            list = JSON.parseArray("[" + args + "]", Object.class);
        } catch (Throwable t) {
            return "Invalid json argument, cause: " + t.getMessage();
        }

        Invoker<?> invoker = null;
        Method invokeMethod = null;
        Collection<Exporter<?>> exporters = DubboProtocol.getDubboProtocol().getExporters();

        // 处理 select 命令的情况
        if (isInvokedSelectCommand(channel)) {
            // 如果是 select 命令，从 channel 中获取之前选择的方法
            invokeMethod = (Method) channel.getAttribute(SelectTelnetHandler.SELECT_METHOD_KEY);
            // 查找对应的 invoker
            for (Exporter<?> exporter : exporters) {
                if (invokeMethod.getDeclaringClass().getName().equals(exporter.getInvoker().getInterface().getName())) {
                    invoker = exporter.getInvoker();
                    break;
                }
            }
        } else {
            // 处理普通调用命令的情况
            
            // 如果没有指定服务名，且不止一个服务，则报错
            if ((StringUtils.isBlank(service))) {
                if (exporters.size() != 1) {
                    return "Failed to find service !";
                }
            }

            // 遍历所有暴露的服务，查找匹配的服务
            for (Exporter<?> exporter : exporters) {
                // 服务名匹配：空、简单类名、全限定类名或 URL 路径
                if (StringUtils.isBlank(service)
                        || service.equals(exporter.getInvoker().getInterface().getSimpleName())
                        || service.equals(exporter.getInvoker().getInterface().getName())
                        || service.equals(exporter.getInvoker().getUrl().getPath())) {
                    invoker = exporter.getInvoker();
                    
                    // 查找匹配的方法（基于方法名和参数个数）
                    List<Method> methodList = findSameSignatureMethod(exporter.getInvoker().getInterface(), method, list);
                    if (CollectionUtils.isNotEmpty(methodList)) {
                        if (methodList.size() == 1) {
                            // 只有一个匹配方法，直接使用
                            invokeMethod = methodList.get(0);
                        } else {
                            // 多个方法名相同，进一步匹配参数类型
                            List<Method> matchMethods = findMatchMethods(methodList, list);
                            if (CollectionUtils.isNotEmpty(matchMethods)) {
                                if (matchMethods.size() == 1) {
                                    // 找到唯一匹配的方法
                                    invokeMethod = matchMethods.get(0);
                                } else { 
                                    // 存在多个重载方法，需要用户手动选择
                                    channel.setAttribute(INVOKE_METHOD_LIST_KEY, matchMethods);
                                    channel.setAttribute(INVOKE_MESSAGE_KEY, message);
                                    printSelectMessage(buf, matchMethods);
                                    return buf.toString();
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }

        // 执行方法调用
        if (invoker != null) {
            if (invokeMethod != null) {
                try {
                    // 将参数列表转换为方法参数类型
                    Object[] array = PojoUtils.realize(list.toArray(), invokeMethod.getParameterTypes(), invokeMethod.getGenericParameterTypes());
                    
                    // 设置 RPC 上下文信息
                    RpcContext.getContext().setLocalAddress(channel.getLocalAddress()).setRemoteAddress(channel.getRemoteAddress());
                    
                    // 记录开始时间
                    long start = System.currentTimeMillis();
                    
                    // 执行远程调用
                    Object result = invoker.invoke(new RpcInvocation(invokeMethod, array)).recreate();
                    
                    // 记录结束时间
                    long end = System.currentTimeMillis();
                    
                    // 格式化输出结果
                    buf.append(JSON.toJSONString(result));
                    buf.append("\r\nelapsed: ");
                    buf.append(end - start);
                    buf.append(" ms.");
                } catch (Throwable t) {
                    // 处理调用异常
                    return "Failed to invoke method " + invokeMethod.getName() + ", cause: " + StringUtils.toString(t);
                }
            } else {
                // 方法不存在
                buf.append("No such method " + method + " in service " + service);
            }
        } else {
            // 服务不存在
            buf.append("No such service " + service);
        }
        return buf.toString();
    }

    /**
     * 查找具有相同方法名和参数个数的方法列表
     * 
     * @param clazz 要查找的类
     * @param lookupMethodName 要查找的方法名
     * @param args 方法参数列表
     * @return 匹配的方法列表
     */
    private List<Method> findSameSignatureMethod(Class clazz, String lookupMethodName, List<Object> args) {
        List<Method> sameSignatureMethods = new ArrayList<Method>();
        Method[] declaredMethods = clazz.getDeclaredMethods();
        for (Method method : declaredMethods) {
            if (method.getName().equals(lookupMethodName) && method.getParameterTypes().length == args.size()) {
                sameSignatureMethods.add(method);
            }
        }
        return sameSignatureMethods;
    }

    /**
     * 从具有相同方法名和参数个数的方法中筛选出参数类型匹配的方法
     * 
     * @param methods 候选方法列表
     * @param args 实际参数列表
     * @return 参数类型匹配的方法列表
     */
    private List<Method> findMatchMethods(List<Method> methods, List<Object> args) {
        List<Method> matchMethod = new ArrayList<Method>();
        for (Method method : methods) {
            if (isMatch(method, args)) {
                matchMethod.add(method);
            }
        }
        return matchMethod;
    }

    /**
     * 检查方法的参数类型是否与实际参数匹配
     * 
     * 匹配规则：
     * 1. 参数个数必须相同
     * 2. 基本类型参数必须完全匹配或可转换
     * 3. null 值可以匹配任何非基本类型
     * 4. 字符串可以匹配枚举类型
     * 5. Map 类型参数根据 class 属性判断类型
     * 6. Collection 类型参数必须是数组或集合类型
     * 
     * @param method 要检查的方法
     * @param args 实际参数列表
     * @return 如果参数类型匹配返回 true，否则返回 false
     */
    private static boolean isMatch(Method method, List<Object> args) {
        Class<?>[] types = method.getParameterTypes();
        if (types.length != args.size()) {
            return false;
        }
        for (int i = 0; i < types.length; i++) {
            Class<?> type = types[i];
            Object arg = args.get(i);

            if (arg == null) {
                if (type.isPrimitive()) {
                    return false;
                }

                // if the type is not primitive, we choose to believe what the invoker want is a null value
                continue;
            }

            if (ReflectUtils.isPrimitive(arg.getClass())) {
                // allow string arg to enum type, @see PojoUtils.realize0()
                if (arg instanceof String && type.isEnum()) {
                    continue;
                }

                if (!ReflectUtils.isPrimitive(type)) {
                    return false;
                }

                if (!ReflectUtils.isCompatible(type, arg)) {
                    return false;
                }
            } else if (arg instanceof Map) {
                String name = (String) ((Map<?, ?>) arg).get("class");
                if (StringUtils.isNotEmpty(name)) {
                    Class<?> cls = ReflectUtils.forName(name);
                    if (!type.isAssignableFrom(cls)) {
                        return false;
                    }
                } else {
                    return true;
                }
            } else if (arg instanceof Collection) {
                if (!type.isArray() && !type.isAssignableFrom(arg.getClass())) {
                    return false;
                }
            } else {
                if (!type.isAssignableFrom(arg.getClass())) {
                    return false;
                }
            }
        }
        return true;
    }


    /**
     * 当存在多个匹配的重载方法时，打印方法列表供用户选择
     * 
     * @param buf 输出缓冲区
     * @param methods 匹配的方法列表
     */
    private void printSelectMessage(StringBuilder buf, List<Method> methods) {
        buf.append("Methods:\r\n");
        for (int i = 0; i < methods.size(); i++) {
            Method method = methods.get(i);
            buf.append(i + 1).append(". ").append(method.getName()).append("(");
            Class<?>[] parameterTypes = method.getParameterTypes();
            for (int n = 0; n < parameterTypes.length; n++) {
                buf.append(parameterTypes[n].getSimpleName());
                if (n != parameterTypes.length - 1) {
                    buf.append(",");
                }
            }
            buf.append(")\r\n");
        }
        buf.append("Please use the select command to select the method you want to invoke. eg: select 1");
    }

    /**
     * 检查是否执行了 select 命令
     * 
     * @param channel 当前的 telnet 通道
     * @return 如果执行了 select 命令返回 true，否则返回 false
     */
    private boolean isInvokedSelectCommand(Channel channel) {
        if (channel.hasAttribute(SelectTelnetHandler.SELECT_KEY)) {
            channel.removeAttribute(SelectTelnetHandler.SELECT_KEY);
            return true;
        }
        return false;
    }
}
