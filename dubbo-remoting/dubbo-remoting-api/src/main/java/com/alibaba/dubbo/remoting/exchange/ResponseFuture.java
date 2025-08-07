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
 * Represents a future response from an asynchronous request. (API/SPI, Prototype, ThreadSafe)
 * 
 * This interface provides both synchronous and asynchronous ways to handle responses:
 * 1. Synchronous: Using get() methods to wait for the response
 * 2. Asynchronous: Using setCallback() to handle the response when it arrives
 * 
 * Key features:
 * - Thread-safe response handling
 * - Timeout support
 * - Callback mechanism
 * - Completion status checking
 * 
 * Typical usage:
 * <pre>
 * // Synchronous
 * ResponseFuture future = channel.request(request);
 * try {
 *     Object result = future.get(timeout);
 *     // handle result
 * } catch (RemotingException e) {
 *     // handle exception
 * }
 * 
 * // Asynchronous
 * future.setCallback(new ResponseCallback() {
 *     public void done(Object result) {
 *         // handle result
 *     }
 *     public void caught(Throwable error) {
 *         // handle error
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
     * Gets the response result, waiting indefinitely if necessary.
     * 
     * This method blocks until:
     * 1. The response is received
     * 2. An error occurs
     * 3. The thread is interrupted
     *
     * @return The response result
     * @throws RemotingException If there is a network or protocol error
     */
    Object get() throws RemotingException;

    /**
     * Gets the response result, waiting up to the specified timeout duration.
     * 
     * This method blocks until:
     * 1. The response is received
     * 2. The timeout period elapses
     * 3. An error occurs
     * 4. The thread is interrupted
     *
     * @param timeoutInMillis Maximum time to wait in milliseconds
     * @return The response result
     * @throws RemotingException If there is a network error or the timeout elapses
     */
    Object get(int timeoutInMillis) throws RemotingException;

    /**
     * Sets a callback for asynchronous response handling.
     * 
     * The callback will be invoked when:
     * 1. The response is received (done method)
     * 2. An error occurs (caught method)
     * 
     * This method provides a non-blocking way to handle responses.
     *
     * @param callback The callback to handle the response or error
     */
    void setCallback(ResponseCallback callback);

    /**
     * Checks if the response has been received.
     * 
     * This method can be used to:
     * 1. Poll for completion without blocking
     * 2. Check if it's safe to call get() without blocking
     * 3. Determine if a request has completed or failed
     *
     * @return true if the response is available or an error occurred,
     *         false if still waiting for the response
     */
    boolean isDone();

}