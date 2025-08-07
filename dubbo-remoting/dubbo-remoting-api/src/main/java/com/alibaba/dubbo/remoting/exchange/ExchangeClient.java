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

import com.alibaba.dubbo.remoting.Client;

/**
 * Client interface that combines request-response capabilities with client functionality. (API/SPI, Prototype, ThreadSafe)
 * 
 * This interface merges the capabilities of Client and ExchangeChannel:
 * 1. Client features:
 *    - Connection management (connect, disconnect, reconnect)
 *    - Client-side configuration
 *    - Connection monitoring
 * 2. ExchangeChannel features:
 *    - Request-response messaging
 *    - Asynchronous operations
 *    - Message handling
 * 
 * ExchangeClient is typically used by Dubbo consumers to:
 * - Establish and maintain connections to providers
 * - Send requests and handle responses
 * - Monitor connection status
 * - Support both synchronous and asynchronous operations
 * 
 * @see com.alibaba.dubbo.remoting.Client
 * @see ExchangeChannel
 */
public interface ExchangeClient extends Client, ExchangeChannel {

}