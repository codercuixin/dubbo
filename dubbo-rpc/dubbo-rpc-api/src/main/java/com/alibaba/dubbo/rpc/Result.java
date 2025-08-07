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
 * Represents the result of an RPC invocation, including return value, exception and attachments. (API, Prototype, NonThreadSafe)
 * 
 * This interface encapsulates all possible outcomes of a remote method call:
 * 1. Normal return value (accessed via getValue())
 * 2. Exception if the call failed (accessed via getException())
 * 3. Additional metadata in the form of attachments
 * 
 * The result object is not thread-safe as it represents a single invocation result
 * and should not be shared between threads.
 *
 * @serial Don't change the class name and package name.
 * @see com.alibaba.dubbo.rpc.Invoker#invoke(Invocation)
 * @see com.alibaba.dubbo.rpc.RpcResult
 */
public interface Result {

    /**
     * Gets the return value of the RPC invocation.
     * 
     * This method returns the actual result returned by the remote service method.
     * If the invocation threw an exception, this method returns null and the
     * exception can be retrieved via getException().
     *
     * @return The invocation result value, or null if there was an exception or no return value
     */
    Object getValue();

    /**
     * Gets the exception that occurred during the RPC invocation, if any.
     * 
     * If the remote service method threw an exception, or if there was a problem
     * during the RPC call itself (like network errors), this method returns that
     * exception. For normal successful calls, this returns null.
     *
     * @return The exception that occurred, or null if the call was successful
     */
    Throwable getException();

    /**
     * Checks if the invocation resulted in an exception.
     * 
     * This is a convenience method that returns true if getException() would
     * return a non-null value. It's useful for quick checks before attempting
     * to access the result value.
     *
     * @return true if the invocation resulted in an exception, false otherwise
     */
    boolean hasException();

    /**
     * Recreates the invocation result by either returning the value or throwing the exception.
     * 
     * This method provides a convenient way to handle the result in a way that matches
     * local method call semantics. It follows this logic:
     * <pre>
     * if (hasException()) {
     *     throw getException();
     * } else {
     *     return getValue();
     * }
     * </pre>
     *
     * @return The invocation result value if the call was successful
     * @throws Throwable The exception that occurred during the invocation if there was one
     */
    Object recreate() throws Throwable;

    /**
     * @see com.alibaba.dubbo.rpc.Result#getValue()
     * @deprecated Replace to getValue()
     */
    @Deprecated
    Object getResult();


    /**
     * Gets all attachments associated with this invocation result.
     * 
     * Attachments can be used to carry additional metadata from the provider
     * back to the consumer, such as tracing data or custom response headers.
     * These attachments are separate from the main return value.
     *
     * @return A Map containing all attachments with string keys and values
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
     */
    String getAttachment(String key, String defaultValue);

}