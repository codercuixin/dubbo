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
 * Invocation represents an RPC call, including method name, parameters and attachments. (API, Prototype, NonThreadSafe)
 * 
 * This interface defines the contract for an RPC invocation, containing all the necessary information
 * to perform a remote method call:
 * 1. Method name and parameter types for method identification
 * 2. Arguments for method invocation
 * 3. Attachments for metadata transfer
 * 4. Context information like the invoker reference
 * 
 * Note: Implementations of this interface are not required to be thread-safe as each invocation
 * typically represents a single RPC call.
 *
 * @serial Don't change the class name and package name.
 * @see com.alibaba.dubbo.rpc.Invoker#invoke(Invocation)
 * @see com.alibaba.dubbo.rpc.RpcInvocation
 */
public interface Invocation {

    /**
     * Gets the name of the method being invoked.
     * 
     * This is the actual method name from the service interface that is being called.
     * For example, if calling "UserService.findById", this would return "findById".
     *
     * @return The method name being invoked
     * @serial The method name must be serializable for RPC transmission
     */
    String getMethodName();

    /**
     * Gets the parameter types of the method being invoked.
     * 
     * This array contains the Class objects representing each parameter's type
     * in the order they appear in the method signature. This is essential for
     * method overloading resolution.
     *
     * @return An array of Class objects representing the parameter types
     * @serial Parameter types must be serializable for RPC transmission
     */
    Class<?>[] getParameterTypes();

    /**
     * Gets the actual argument values for the method invocation.
     * 
     * This array contains the actual parameter values to be passed to the method
     * in the order they appear in the method signature. The arguments must match
     * the parameter types returned by getParameterTypes().
     *
     * @return An array of Objects containing the actual argument values
     * @serial Arguments must be serializable for RPC transmission
     */
    Object[] getArguments();

    /**
     * Gets all attachments carried with this invocation.
     * 
     * Attachments are used to carry additional metadata for the invocation,
     * such as tracing context, authentication tokens, or any other metadata
     * that needs to be transferred between consumer and provider.
     *
     * @return A Map containing all attachments with string keys and values
     * @serial Attachments must be serializable for RPC transmission
     */
    Map<String, String> getAttachments();

    /**
     * Gets a specific attachment value by its key.
     * 
     * This is a convenience method to get a single attachment value
     * without having to handle the map directly.
     *
     * @param key The key of the attachment to retrieve
     * @return The attachment value associated with the key, or null if not found
     * @serial The attachment value must be serializable for RPC transmission
     */
    String getAttachment(String key);

    /**
     * Gets a specific attachment value by its key, returning a default value if not found.
     * 
     * This is a convenience method similar to getAttachment(String), but allows
     * specifying a default value to return when the key is not found.
     *
     * @param key The key of the attachment to retrieve
     * @param defaultValue The value to return if the key is not found
     * @return The attachment value associated with the key, or defaultValue if not found
     * @serial The attachment value must be serializable for RPC transmission
     */
    String getAttachment(String key, String defaultValue);

    /**
     * Gets the invoker associated with this invocation in the current context.
     * 
     * The invoker represents the service provider that will handle this invocation.
     * This is particularly useful in filter chains and other intercepting code
     * to access the target service provider.
     *
     * @return The Invoker instance that will handle this invocation
     * @transient This value should not be serialized as it's context-specific
     */
    Invoker<?> getInvoker();

    /**
     * Stores a value in the invocation's attribute map.
     * 
     * Attributes differ from attachments in that they are not transmitted during RPC calls.
     * They are used for storing context data that is only relevant within the current JVM.
     *
     * @param key The key to store the value under
     * @param value The value to store
     * @return The previous value associated with the key, or null if there was no previous value
     */
    Object put(Object key, Object value);

    /**
     * Retrieves a value from the invocation's attribute map.
     * 
     * @param key The key of the value to retrieve
     * @return The value associated with the key, or null if not found
     */
    Object get(Object key);

    /**
     * Gets all attributes stored in this invocation.
     * 
     * Returns a map of all local attributes. Unlike attachments, these attributes
     * are not transmitted during RPC calls and are only valid within the current JVM.
     *
     * @return A Map containing all attributes
     */
    Map<Object, Object> getAttributes();
}