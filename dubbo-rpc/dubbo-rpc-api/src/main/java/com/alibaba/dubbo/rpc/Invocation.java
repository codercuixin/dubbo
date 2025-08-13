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
package com.alibaba.dubbo.rpc;

import java.util.Map;

/**
 * RPC调用信息封装接口。(API, Prototype, NonThreadSafe)
 * 
 * 该接口封装了一次RPC调用所需的所有信息：
 * 1. 方法标识：
 *    - 方法名
 *    - 参数类型列表
 * 2. 调用参数：
 *    - 实际参数值
 *    - 参数类型检查
 * 3. 元数据传输：
 *    - 附加信息（attachments）
 *    - 上下文数据
 * 4. 调用上下文：
 *    - Invoker引用
 *    - 本地属性
 * 
 * 主要特性：
 * - 非线程安全：每个实例代表单次调用
 * - 可序列化：支持网络传输
 * - 轻量级：避免存储过多状态
 * - 支持泛化调用
 * 
 * 使用场景：
 * 1. 服务方法调用
 * 2. 泛化调用支持
 * 3. 调用拦截和过滤
 * 4. 调用上下文传递
 * 
 * @serial 不要修改类名和包名
 * @see com.alibaba.dubbo.rpc.Invoker#invoke(Invocation)
 * @see com.alibaba.dubbo.rpc.RpcInvocation
 */
public interface Invocation {

    /**
     * 获取被调用的方法名。
     * 
     * 返回服务接口中实际被调用的方法名：
     * - 例如调用"UserService.findById"时，返回"findById"
     * - 例如调用"OrderService.create"时，返回"create"
     * 
     * 使用场景：
     * 1. 方法调用分发
     * 2. 方法级别的拦截
     * 3. 方法调用统计
     *
     * @return 被调用的方法名
     * @serial 方法名必须可序列化以支持RPC传输
     */
    String getMethodName();

    /**
     * 获取方法的参数类型列表。
     * 
     * 返回一个Class数组，按方法签名中参数的顺序包含每个参数的类型：
     * - 用于方法重载解析
     * - 支持参数类型检查
     * - 辅助参数序列化
     * 
     * 使用场景：
     * 1. 方法重载匹配
     * 2. 参数类型验证
     * 3. 泛化调用支持
     * 4. 参数序列化
     *
     * @return 参数类型的Class对象数组
     * @serial 参数类型必须可序列化以支持RPC传输
     */
    Class<?>[] getParameterTypes();

    /**
     * 获取方法调用的实际参数值。
     * 
     * 返回一个对象数组，包含按方法签名顺序排列的实际参数值：
     * - 参数值必须与getParameterTypes()返回的类型匹配
     * - 支持基本类型和复杂对象
     * - 需要确保参数可序列化
     * 
     * 使用场景：
     * 1. 方法调用执行
     * 2. 参数值验证
     * 3. 参数日志记录
     * 4. 参数值转换
     *
     * @return 包含实际参数值的对象数组
     * @serial 参数值必须可序列化以支持RPC传输
     */
    Object[] getArguments();

    /**
     * 获取调用携带的所有附加信息。
     * 
     * 附加信息用于传输调用的元数据：
     * - 链路追踪上下文
     * - 认证令牌
     * - 超时配置
     * - 版本信息
     * - 分组信息
     * - 其他需要在消费者和提供者间传递的元数据
     * 
     * 使用场景：
     * 1. 分布式追踪
     * 2. 服务鉴权
     * 3. 流量控制
     * 4. 服务治理
     *
     * @return 包含所有附加信息的Map，键和值都是字符串
     * @serial 附加信息必须可序列化以支持RPC传输
     */
    Map<String, String> getAttachments();

    /**
     * 根据键获取特定的附加信息值。
     * 
     * 这是一个便捷方法，无需直接操作Map即可获取单个附加值：
     * - 如果键不存在返回null
     * - 常用于获取特定的元数据
     * - 支持常见的配置项
     * 
     * 常用的附加信息键：
     * - interface：服务接口名
     * - version：服务版本
     * - group：服务分组
     * - timeout：调用超时时间
     * - token：认证令牌
     *
     * @param key 要获取的附加信息的键
     * @return 与键关联的附加信息值，如果未找到则返回null
     * @serial 附加信息值必须可序列化以支持RPC传输
     */
    String getAttachment(String key);

    /**
     * 根据键获取附加信息值，如果未找到则返回默认值。
     * 
     * 这是getAttachment(String)的增强版本：
     * - 支持指定默认值
     * - 避免空值判断
     * - 简化配置获取
     * 
     * 使用场景：
     * 1. 获取配置项时指定默认值
     * 2. 处理可选的元数据
     * 3. 确保返回有效值
     * 
     * 示例：
     * - getAttachment("timeout", "5000")
     * - getAttachment("version", "1.0.0")
     * - getAttachment("loadbalance", "random")
     *
     * @param key 要获取的附加信息的键
     * @param defaultValue 当键不存在时返回的默认值
     * @return 与键关联的附加信息值，如果未找到则返回defaultValue
     * @serial 附加信息值必须可序列化以支持RPC传输
     */
    String getAttachment(String key, String defaultValue);

    /**
     * 获取当前上下文中与此调用关联的Invoker。
     * 
     * Invoker代表了将处理此调用的服务提供者：
     * - 包含服务的具体实现
     * - 维护调用的上下文信息
     * - 处理实际的调用过程
     * 
     * 使用场景：
     * 1. 过滤器链中访问目标服务
     * 2. 拦截器中获取服务信息
     * 3. 获取服务配置和元数据
     * 4. 服务调用监控和统计
     *
     * @return 处理此调用的Invoker实例
     * @transient 此值不应被序列化，因为它是上下文相关的
     */
    Invoker<?> getInvoker();

    /**
     * 在调用的属性映射中存储值。
     * 
     * 属性与附加信息(attachments)的区别：
     * - 属性不会在RPC调用中传输
     * - 仅在当前JVM中有效
     * - 用于存储本地上下文数据
     * - 支持任意类型的值
     * 
     * 使用场景：
     * 1. 存储调用链路信息
     * 2. 保存中间处理结果
     * 3. 传递本地上下文
     * 4. 缓存临时数据
     *
     * @param key 存储值的键
     * @param value 要存储的值
     * @return 与键关联的前一个值，如果没有则返回null
     */
    Object put(Object key, Object value);

    /**
     * 从调用的属性映射中获取值。
     * 
     * 此方法用于：
     * - 获取之前存储的本地属性
     * - 访问上下文数据
     * - 读取中间结果
     * 
     * 注意事项：
     * - 返回值可能为null
     * - 属性仅在本地有效
     * - 支持任意类型的值
     *
     * @param key 要获取的值的键
     * @return 与键关联的值，如果未找到则返回null
     */
    Object get(Object key);

    /**
     * 获取此调用中存储的所有属性。
     * 
     * 返回包含所有本地属性的映射：
     * - 与attachments不同，这些属性不会在RPC调用中传输
     * - 仅在当前JVM中有效
     * - 用于本地上下文管理
     * - 支持任意类型的键值对
     * 
     * 使用场景：
     * 1. 批量获取上下文数据
     * 2. 调试和监控
     * 3. 状态快照
     * 4. 上下文传递
     *
     * @return 包含所有属性的Map
     */
    Map<Object, Object> getAttributes();
}