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

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.exchange.support.header.HeaderExchanger;

/**
 * Factory interface for creating exchange clients and servers. (SPI, Singleton, ThreadSafe)
 * 
 * The Exchanger is responsible for creating network communication endpoints that support
 * the request-response message exchange pattern. It provides two main functionalities:
 * 1. Creating servers that accept client connections
 * 2. Creating clients that connect to servers
 * 
 * Key features:
 * - SPI extensible: Different implementations can be plugged in
 * - Thread-safe: Can be safely shared across threads
 * - Singleton: One instance per implementation is sufficient
 * - Supports both synchronous and asynchronous communication
 * 
 * The default implementation is HeaderExchanger, which adds a message header
 * for request-response correlation.
 * 
 * @see <a href="http://en.wikipedia.org/wiki/Message_Exchange_Pattern">Message Exchange Pattern</a>
 * @see <a href="http://en.wikipedia.org/wiki/Request-response">Request-Response Pattern</a>
 * @see ExchangeClient
 * @see ExchangeServer
 * @see HeaderExchanger
 */
@SPI(HeaderExchanger.NAME)
public interface Exchanger {

    /**
     * Creates and binds an exchange server to the specified URL.
     * 
     * This method:
     * 1. Creates a new exchange server instance
     * 2. Binds it to the port specified in the URL
     * 3. Configures it with the provided handler
     * 4. Starts accepting client connections
     * 
     * The server configuration is specified through the URL parameters.
     *
     * @param url The URL containing server configuration parameters
     * @param handler The handler that will process incoming requests
     * @return A new ExchangeServer instance that is ready to accept connections
     * @throws RemotingException If the server cannot be created or bound
     */
    @Adaptive({Constants.EXCHANGER_KEY})
    ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException;

    /**
     * Creates and connects an exchange client to a remote server.
     * 
     * This method:
     * 1. Creates a new exchange client instance
     * 2. Connects it to the server specified in the URL
     * 3. Configures it with the provided handler
     * 4. Establishes the connection and returns when ready
     * 
     * The client configuration is specified through the URL parameters.
     * The connection attempt will timeout based on URL parameters.
     *
     * @param url The URL containing server address and client configuration
     * @param handler The handler that will process server responses
     * @return A new ExchangeClient instance that is connected and ready
     * @throws RemotingException If the client cannot connect to the server
     */
    @Adaptive({Constants.EXCHANGER_KEY})
    ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException;

}