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

import com.alibaba.dubbo.remoting.Server;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Server interface that supports request-response messaging. (API/SPI, Prototype, ThreadSafe)
 * 
 * This interface extends the basic Server with exchange capabilities:
 * 1. Server features:
 *    - Bind to port and accept connections
 *    - Connection management
 *    - Server lifecycle control
 * 2. Exchange features:
 *    - Request-response messaging
 *    - Channel management
 *    - Remote address based channel lookup
 * 
 * ExchangeServer is typically used by Dubbo providers to:
 * - Accept and manage client connections
 * - Process incoming requests and send responses
 * - Monitor connected clients
 * - Support both synchronous and asynchronous operations
 * 
 * @see com.alibaba.dubbo.remoting.Server
 * @see ExchangeChannel
 */
public interface ExchangeServer extends Server {

    /**
     * Gets all exchange channels currently connected to this server.
     * 
     * This method provides access to all active client connections, allowing:
     * - Connection monitoring
     * - Broadcast messages to all clients
     * - Connection statistics and management
     *
     * @return Collection of all active exchange channels
     */
    Collection<ExchangeChannel> getExchangeChannels();

    /**
     * Gets the exchange channel for a specific remote address.
     * 
     * This method allows direct access to a specific client connection by its address.
     * Useful for:
     * - Targeted message sending
     * - Connection monitoring
     * - Client-specific operations
     *
     * @param remoteAddress The socket address of the remote client
     * @return The exchange channel to that client, or null if not connected
     */
    ExchangeChannel getExchangeChannel(InetSocketAddress remoteAddress);

}