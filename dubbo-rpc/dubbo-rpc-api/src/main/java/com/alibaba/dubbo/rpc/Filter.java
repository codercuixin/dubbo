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

import com.alibaba.dubbo.common.extension.SPI;

/**
 * RPC调用拦截器接口。(SPI, Singleton, ThreadSafe)
 * 
 * Filter在RPC调用过程中形成拦截链：
 * 1. 调用前处理：
 *    - 参数验证
 *    - 请求日志
 *    - 权限检查
 * 2. 调用后处理：
 *    - 结果处理
 *    - 响应日志
 *    - 统计信息
 * 3. 异常处理：
 *    - 异常转换
 *    - 错误恢复
 *    - 降级处理
 * 
 * 主要特性：
 * - 可扩展的：支持SPI机制
 * - 线程安全：支持并发调用
 * - 链式处理：多个Filter串联
 * - 双向拦截：请求和响应
 * 
 * 常见用途：
 * 1. 安全控制
 *    - 身份认证
 *    - 权限校验
 *    - 访问控制
 * 2. 性能优化
 *    - 结果缓存
 *    - 限流降级
 *    - 超时控制
 * 3. 日志监控
 *    - 调用日志
 *    - 性能统计
 *    - 链路追踪
 * 4. 数据处理
 *    - 参数验证
 *    - 结果转换
 *    - 协议适配
 * 
 * @see com.alibaba.dubbo.rpc.Invoker
 * @see com.alibaba.dubbo.rpc.Invocation
 */
@SPI
public interface Filter {

    /**
     * 拦截RPC调用并执行自定义逻辑。
     * 
     * 典型的Filter实现模式如下：
     * <pre>
     * public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
     *     // 调用前处理（如：日志记录、参数验证）
     *     try {
     *         // 执行实际的调用
     *         Result result = invoker.invoke(invocation);
     *         // 成功结果的后处理
     *         return result;
     *     } catch (RpcException e) {
     *         // 异常处理逻辑
     *         throw e;
     *     } finally {
     *         // 必要的清理工作
     *     }
     * }
     * </pre>
     * 
     * 处理流程：
     * 1. 前置处理：
     *    - 请求参数验证
     *    - 请求日志记录
     *    - 权限认证检查
     * 2. 调用执行：
     *    - 调用下一个Filter
     *    - 最终调用目标方法
     * 3. 后置处理：
     *    - 结果验证和转换
     *    - 响应日志记录
     *    - 统计信息更新
     * 4. 异常处理：
     *    - 异常捕获和转换
     *    - 错误日志记录
     *    - 失败统计和告警
     *
     * @param invoker 代表目标服务的Invoker对象
     * @param invocation 包含方法调用详情的Invocation对象
     * @return RPC调用的结果，可能被Filter修改过
     * @throws RpcException 当Filter处理过程中发生错误时抛出
     * @see com.alibaba.dubbo.rpc.Invoker#invoke(Invocation)
     */
    Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException;

}