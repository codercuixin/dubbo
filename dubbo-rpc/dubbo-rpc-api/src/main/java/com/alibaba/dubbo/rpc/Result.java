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
 * RPC调用结果的封装接口。(API, Prototype, NonThreadSafe)
 * 
 * 该接口封装了远程方法调用的所有可能结果：
 * 1. 正常返回值：
 *    - 通过getValue()访问
 *    - 类型与方法声明匹配
 * 2. 异常情况：
 *    - 通过getException()访问
 *    - 包括业务异常和RPC异常
 * 3. 附加元数据：
 *    - 通过attachments传递
 *    - 支持上下文信息传递
 * 
 * 主要特性：
 * - 非线程安全：每个实例代表单次调用结果
 * - 结果重建：支持异常传播和结果重构
 * - 类型安全：保证返回值类型匹配
 * - 元数据传递：支持附加信息传输
 * 
 * 使用场景：
 * 1. 远程调用结果处理
 * 2. 异常处理和传播
 * 3. 调用跟踪和监控
 * 4. 上下文信息传递
 * 
 * @serial 不要修改类名和包名
 * @see com.alibaba.dubbo.rpc.Invoker#invoke(Invocation)
 * @see com.alibaba.dubbo.rpc.RpcResult
 */
public interface Result {

    /**
     * 获取RPC调用的返回值。
     * 
     * 此方法返回远程服务方法的实际结果：
     * - 返回值类型与方法声明匹配
     * - 如果调用出现异常，返回null
     * - 异常信息可通过getException()获取
     * - void方法返回null
     * 
     * 使用场景：
     * 1. 获取正常调用结果
     * 2. 结果类型转换
     * 3. 空值处理
     *
     * @return 调用结果值，如果有异常或无返回值则返回null
     */
    Object getValue();

    /**
     * 获取RPC调用过程中发生的异常（如果有）。
     * 
     * 此方法返回调用过程中的异常：
     * 1. 业务异常：
     *    - 服务方法抛出的异常
     *    - 参数验证异常
     * 2. RPC异常：
     *    - 网络通信错误
     *    - 序列化异常
     *    - 超时异常
     * 
     * 使用场景：
     * 1. 异常处理和恢复
     * 2. 错误日志记录
     * 3. 异常信息提取
     * 4. 故障诊断
     *
     * @return 发生的异常，如果调用成功则返回null
     */
    Throwable getException();

    /**
     * 检查调用是否产生了异常。
     * 
     * 这是一个便捷方法，用于快速检查调用结果：
     * - 等同于getException() != null
     * - 用于结果处理前的快速检查
     * - 避免不必要的异常处理
     * 
     * 使用场景：
     * 1. 条件分支处理
     * 2. 异常检查
     * 3. 结果验证
     * 4. 快速失败检查
     *
     * @return 如果调用产生异常返回true，否则返回false
     */
    boolean hasException();

    /**
     * 重建调用结果，返回结果值或抛出异常。
     * 
     * 此方法提供了一种与本地方法调用语义匹配的处理方式：
     * <pre>
     * if (hasException()) {
     *     throw getException();  // 有异常则抛出
     * } else {
     *     return getValue();     // 无异常则返回结果
     * }
     * </pre>
     * 
     * 使用场景：
     * 1. 模拟本地方法调用
     * 2. 统一的结果处理
     * 3. 异常传播控制
     * 4. 结果重建和转换
     * 
     * 注意事项：
     * - 异常会被直接抛出
     * - 可能抛出未检查异常
     * - 需要适当的异常处理
     *
     * @return 如果调用成功则返回结果值
     * @throws Throwable 如果调用过程中发生异常则抛出该异常
     */
    Object recreate() throws Throwable;

    /**
     * 获取调用结果值。
     * 
     * @see com.alibaba.dubbo.rpc.Result#getValue()
     * @deprecated 请使用getValue()方法代替
     */
    @Deprecated
    Object getResult();


    /**
     * 获取与此调用结果关联的所有附加信息。
     * 
     * 附加信息用于从提供者传递额外的元数据到消费者：
     * - 链路追踪数据
     * - 自定义响应头
     * - 性能统计信息
     * - 服务治理数据
     * 
     * 特点：
     * - 独立于主要返回值
     * - 支持字符串键值对
     * - 可序列化传输
     * - 用于扩展信息传递
     *
     * @return 包含所有附加信息的Map，键和值都是字符串
     */
    Map<String, String> getAttachments();

    /**
     * 根据键获取特定的附加信息值。
     * 
     * 这是一个便捷方法，无需直接操作Map即可获取单个附加值：
     * - 如果键不存在返回null
     * - 用于获取特定元数据
     * - 简化附加信息访问
     * 
     * 常用的附加信息键：
     * - dubbo.response.time：响应时间
     * - dubbo.trace.id：调用链ID
     * - dubbo.error.code：错误码
     * - dubbo.version：服务版本
     *
     * @param key 要获取的附加信息的键
     * @return 与键关联的附加信息值，如果未找到则返回null
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
     * 1. 获取可选的元数据
     * 2. 设置默认配置
     * 3. 处理降级参数
     * 4. 确保返回有效值
     * 
     * 示例：
     * - getAttachment("timeout", "5000")
     * - getAttachment("retries", "2")
     * - getAttachment("loadbalance", "random")
     *
     * @param key 要获取的附加信息的键
     * @param defaultValue 当键不存在时返回的默认值
     * @return 与键关联的附加信息值，如果未找到则返回defaultValue
     */
    String getAttachment(String key, String defaultValue);

}