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
 * Callback interface for asynchronous response handling. (API, Prototype, ThreadSafe)
 * 
 * This interface defines the contract for handling asynchronous responses:
 * 1. Successful responses through done()
 * 2. Error conditions through caught()
 * 
 * The callback methods are invoked when:
 * - The response is received from the remote endpoint
 * - An error occurs during request processing
 * - The request times out
 * 
 * Implementation considerations:
 * - Methods should return quickly to avoid blocking
 * - Implementations should be thread-safe
 * - Exceptions in callback methods are logged but not propagated
 * 
 * @see ResponseFuture#setCallback(ResponseCallback)
 */
public interface ResponseCallback {

    /**
     * Called when a response is successfully received.
     * 
     * This method is invoked when:
     * 1. The response arrives normally
     * 2. The response can be successfully deserialized
     * 3. No timeout or other errors occurred
     * 
     * The implementation should:
     * 1. Process the response quickly
     * 2. Not throw exceptions
     * 3. Handle null responses appropriately
     *
     * @param response The response object from the remote call
     */
    void done(Object response);

    /**
     * Called when an error occurs during response processing.
     * 
     * This method is invoked when:
     * 1. Network errors occur
     * 2. Timeout occurs
     * 3. Response deserialization fails
     * 4. Other processing errors occur
     * 
     * The implementation should:
     * 1. Handle the error appropriately
     * 2. Not throw exceptions
     * 3. Log errors if necessary
     * 4. Clean up any resources
     *
     * @param exception The error that occurred during processing
     */
    void caught(Throwable exception);

}