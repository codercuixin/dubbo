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

import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;

/**
 * Extended channel interface that supports request-response style communication. (API/SPI, Prototype, ThreadSafe)
 * 
 * This interface extends the basic Channel interface to provide:
 * 1. Request-response pattern support through request() methods
 * 2. Asynchronous response handling via ResponseFuture
 * 3. Exchange message handling capabilities
 * 4. Graceful shutdown support
 * 
 * ExchangeChannel is the foundation for RPC communication in Dubbo, enabling
 * both synchronous and asynchronous request-response patterns over a network connection.
 * 
 * @see com.alibaba.dubbo.remoting.Channel
 * @see ResponseFuture
 * @see ExchangeHandler
 */
public interface ExchangeChannel extends Channel {

    /**
     * Sends a request message and returns a future for the response.
     * 
     * This method implements the asynchronous request-response pattern:
     * 1. Sends the request message to the remote endpoint
     * 2. Returns immediately with a ResponseFuture
     * 3. The response can be retrieved later through the ResponseFuture
     * 
     * Uses the default timeout value configured for the channel.
     *
     * @param request The request message to send
     * @return A ResponseFuture that will contain the response when it arrives
     * @throws RemotingException If the request cannot be sent
     */
    ResponseFuture request(Object request) throws RemotingException;

    /**
     * Sends a request message with a specified timeout and returns a future for the response.
     * 
     * Similar to request(Object), but allows specifying a custom timeout:
     * 1. Sends the request message to the remote endpoint
     * 2. Returns immediately with a ResponseFuture
     * 3. The response must arrive within the specified timeout
     * 4. If timeout occurs, the future will complete exceptionally
     *
     * @param request The request message to send
     * @param timeout Maximum time to wait for response in milliseconds
     * @return A ResponseFuture that will contain the response when it arrives
     * @throws RemotingException If the request cannot be sent
     */
    ResponseFuture request(Object request, int timeout) throws RemotingException;

    /**
     * Gets the exchange handler associated with this channel.
     * 
     * The exchange handler is responsible for processing incoming requests
     * and responses on this channel. It contains the business logic for
     * handling different types of messages.
     *
     * @return The ExchangeHandler that processes messages for this channel
     */
    ExchangeHandler getExchangeHandler();

    /**
     * Closes the channel gracefully with a specified timeout.
     * 
     * A graceful close ensures that:
     * 1. No new requests are accepted
     * 2. Pending requests are allowed to complete
     * 3. Resources are properly released
     * 4. The underlying connection is closed after cleanup
     *
     * @param timeout Maximum time in milliseconds to wait for pending requests
     */
    @Override
    void close(int timeout);

}
