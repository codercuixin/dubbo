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

/**
 * 异步响应处理的回调接口。(API, Prototype, ThreadSafe)
 * 
 * 该接口定义了处理异步响应的契约：
 * 1. 通过 done() 处理成功的响应
 * 2. 通过 caught() 处理错误情况
 * 
 * 回调方法在以下情况下被调用：
 * - 从远程端点接收到响应时
 * - 请求处理过程中发生错误时
 * - 请求超时时
 * 
 * 实现注意事项：
 * - 方法应快速返回以避免阻塞
 * - 实现应该是线程安全的
 * - 回调方法中的异常会被记录日志但不会传播
 * 
 * @see ResponseFuture#setCallback(ResponseCallback)
 */
public interface ResponseCallback {

    /**
     * 当成功接收到响应时调用。
     * 
     * 此方法在以下情况下被调用：
     * 1. 响应正常到达
     * 2. 响应可以被成功反序列化
     * 3. 没有发生超时或其他错误
     * 
     * 实现应该：
     * 1. 快速处理响应
     * 2. 不抛出异常
     * 3. 适当处理空响应
     *
     * @param response 来自远程调用的响应对象
     */
    void done(Object response);

    /**
     * 当响应处理过程中发生错误时调用。
     * 
     * 此方法在以下情况下被调用：
     * 1. 发生网络错误
     * 2. 发生超时
     * 3. 响应反序列化失败
     * 4. 发生其他处理错误
     * 
     * 实现应该：
     * 1. 适当处理错误
     * 2. 不抛出异常
     * 3. 必要时记录错误日志
     * 4. 清理所有资源
     *
     * @param exception 处理过程中发生的错误
     */
    void caught(Throwable exception);

}