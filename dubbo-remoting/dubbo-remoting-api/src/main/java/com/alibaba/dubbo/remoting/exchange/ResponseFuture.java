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
package com.alibaba.dubbo.remoting.exchange;

import com.alibaba.dubbo.remoting.RemotingException;

/**
 * 表示异步请求的未来响应。(API/SPI, Prototype, ThreadSafe)
 * 
 * 该接口提供同步和异步两种方式处理响应：
 * 1. 同步：使用get()方法等待响应
 * 2. 异步：使用setCallback()在响应到达时处理
 * 
 * 主要特性：
 * - 线程安全的响应处理
 * - 超时支持
 * - 回调机制
 * - 完成状态检查
 * 
 * 典型用法：
 * <pre>
 * // 同步方式
 * ResponseFuture future = channel.request(request);
 * try {
 *     Object result = future.get(timeout);
 *     // 处理结果
 * } catch (RemotingException e) {
 *     // 处理异常
 * }
 * 
 * // 异步方式
 * future.setCallback(new ResponseCallback() {
 *     public void done(Object result) {
 *         // 处理结果
 *     }
 *     public void caught(Throwable error) {
 *         // 处理错误
 *     }
 * });
 * </pre>
 * 
 * @see com.alibaba.dubbo.remoting.exchange.ExchangeChannel#request(Object)
 * @see com.alibaba.dubbo.remoting.exchange.ExchangeChannel#request(Object, int)
 * @see ResponseCallback
 */
public interface ResponseFuture {

    /**
     * 获取响应结果，如果需要则无限期等待。
     * 
     * 此方法会阻塞直到：
     * 1. 收到响应
     * 2. 发生错误
     * 3. 线程被中断
     *
     * @return 响应结果
     * @throws RemotingException 如果发生网络或协议错误
     */
    Object get() throws RemotingException;

    /**
     * 获取响应结果，最多等待指定的超时时间。
     * 
     * 此方法会阻塞直到：
     * 1. 收到响应
     * 2. 超时时间到期
     * 3. 发生错误
     * 4. 线程被中断
     *
     * @param timeoutInMillis 最大等待时间（毫秒）
     * @return 响应结果
     * @throws RemotingException 如果发生网络错误或超时
     */
    Object get(int timeoutInMillis) throws RemotingException;

    /**
     * 设置用于异步响应处理的回调。
     * 
     * 回调会在以下情况被调用：
     * 1. 收到响应时（done方法）
     * 2. 发生错误时（caught方法）
     * 
     * 此方法提供了一种非阻塞的方式来处理响应。
     *
     * @param callback 用于处理响应或错误的回调
     */
    void setCallback(ResponseCallback callback);

    /**
     * 检查是否已收到响应。
     * 
     * 此方法可用于：
     * 1. 在不阻塞的情况下轮询完成状态
     * 2. 检查是否可以安全地调用get()而不会阻塞
     * 3. 判断请求是否已完成或失败
     *
     * @return 如果响应已到达或发生错误则返回true，
     *         如果仍在等待响应则返回false
     */
    boolean isDone();

}