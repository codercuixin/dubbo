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

import com.alibaba.dubbo.common.Node;

/**
 * Invoker is the core abstraction in Dubbo. It represents an executable service instance
 * for both provider and consumer side. (API/SPI, Prototype, ThreadSafe)
 * 
 * For a provider, an Invoker wraps the service implementation and handles RPC requests.
 * For a consumer, an Invoker represents a proxy that sends RPC requests to remote providers.
 * 
 * The lifecycle of an Invoker ties to the service it represents:
 * 1. Created by Protocol implementation when service is exported/referred
 * 2. Destroyed when service is unexported/unreferred
 * 
 * Thread-safety is required since an Invoker will process requests concurrently.
 *
 * @param <T> The service interface type
 * @see com.alibaba.dubbo.rpc.Protocol#refer(Class, com.alibaba.dubbo.common.URL)
 * @see com.alibaba.dubbo.rpc.InvokerListener
 * @see com.alibaba.dubbo.rpc.protocol.AbstractInvoker
 */
public interface Invoker<T> extends Node {

    /**
     * Gets the interface of the service being invoked.
     * 
     * This method returns the actual service interface class that this Invoker
     * is handling. For example, if this Invoker is for a UserService,
     * this method will return the UserService.class.
     *
     * @return The service interface class
     */
    Class<T> getInterface();

    /**
     * Executes the RPC invocation.
     * 
     * This is the core method that handles RPC calls. For providers, it executes
     * the actual service method. For consumers, it sends the request to the remote provider.
     * 
     * The implementation should handle the following:
     * 1. Parameter validation
     * 2. Service method invocation or remote call
     * 3. Exception handling and conversion
     * 4. Result serialization/deserialization
     *
     * @param invocation Contains the method name, parameter types and values to be invoked
     * @return The invocation result, contains the actual return value or exception
     * @throws RpcException when any error occurs during the invocation, such as network errors
     */
    Result invoke(Invocation invocation) throws RpcException;

}