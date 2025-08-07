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

import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.telnet.TelnetHandler;

/**
 * Handler for processing request-response style messages. (API, Prototype, ThreadSafe)
 * 
 * This interface combines capabilities from both ChannelHandler and TelnetHandler:
 * 1. Basic channel event handling (connect, disconnect, send, receive)
 * 2. Telnet command processing for remote management
 * 3. Request-response message handling
 * 
 * ExchangeHandler is a key component in Dubbo's remoting layer:
 * - It processes business requests and returns responses
 * - It handles connection lifecycle events
 * - It supports telnet access for administration
 * 
 * Implementations must be thread-safe as they will be called concurrently.
 * 
 * @see com.alibaba.dubbo.remoting.ChannelHandler
 * @see com.alibaba.dubbo.remoting.telnet.TelnetHandler
 * @see ExchangeChannel
 */
public interface ExchangeHandler extends ChannelHandler, TelnetHandler {

    /**
     * Processes a request and returns a response.
     * 
     * This method is the core of request-response processing:
     * 1. It receives a request from a remote peer
     * 2. Processes the request according to business logic
     * 3. Returns a response that will be sent back to the requester
     * 
     * The implementation should handle these aspects:
     * - Parameter validation
     * - Request deserialization
     * - Business logic execution
     * - Response serialization
     * - Error handling
     *
     * @param channel The exchange channel that received the request
     * @param request The request object to process
     * @return The response object that will be sent back to the requester
     * @throws RemotingException If any error occurs during request processing
     */
    Object reply(ExchangeChannel channel, Object request) throws RemotingException;

}