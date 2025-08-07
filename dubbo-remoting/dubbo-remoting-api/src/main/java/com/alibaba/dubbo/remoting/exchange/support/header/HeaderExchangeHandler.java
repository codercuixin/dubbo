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
package com.alibaba.dubbo.remoting.exchange.support.header;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.ExecutionException;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.exchange.ExchangeChannel;
import com.alibaba.dubbo.remoting.exchange.ExchangeHandler;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.support.DefaultFuture;
import com.alibaba.dubbo.remoting.transport.ChannelHandlerDelegate;

import java.net.InetSocketAddress;

/**
 * Exchange message handler that handles message exchange between client and server.
 * 
 * This handler is responsible for processing various types of messages:
 * 1. Request/Response handling for RPC calls
 * 2. Event messages (e.g. readonly events)
 * 3. Heartbeat messages
 * 4. Telnet commands
 * 
 * The handler maintains timestamps for read/write operations and properly manages
 * channel lifecycle. It delegates actual business logic to an underlying 
 * {@link ExchangeHandler}.
 * 
 * Thread safety: This class is thread-safe as it is stateless and all mutable
 * state is maintained in thread-safe channel attributes.
 */
public class HeaderExchangeHandler implements ChannelHandlerDelegate {

    protected static final Logger logger = LoggerFactory.getLogger(HeaderExchangeHandler.class);

    public static String KEY_READ_TIMESTAMP = HeartbeatHandler.KEY_READ_TIMESTAMP;

    public static String KEY_WRITE_TIMESTAMP = HeartbeatHandler.KEY_WRITE_TIMESTAMP;

    private final ExchangeHandler handler;

    public HeaderExchangeHandler(ExchangeHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.handler = handler;
    }

    /**
     * Handles response messages by notifying waiting threads.
     * 
     * This method processes responses received from the remote end:
     * - Ignores heartbeat responses
     * - For normal responses, notifies the corresponding DefaultFuture
     *   which will wake up threads waiting for the response
     * 
     * This is a key part of Dubbo's async response handling mechanism.
     *
     * @param channel The channel on which the response was received
     * @param response The response to handle
     * @throws RemotingException If any error occurs during response handling
     */
    static void handleResponse(Channel channel, Response response) throws RemotingException {
        if (response != null && !response.isHeartbeat()) {
            DefaultFuture.received(channel, response);
        }
    }

    /**
     * Determines if the given channel is on the client side.
     * 
     * This is determined by comparing the channel's URL port with the remote address port,
     * and checking if the IP addresses match (after filtering for localhost variations).
     * 
     * This method is particularly important for handling string messages differently
     * on client and server sides - clients don't support string messages (telnet).
     *
     * @param channel The channel to check
     * @return true if the channel is on the client side, false otherwise
     */
    private static boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(url.getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    /**
     * Handles event type requests.
     * 
     * Currently supports:
     * - READONLY_EVENT: Marks the channel as read-only by setting a channel attribute
     * 
     * Events are special requests that modify channel behavior or state rather
     * than performing regular RPC operations.
     *
     * @param channel The channel on which the event was received
     * @param req The event request to handle
     * @throws RemotingException If any error occurs during event handling
     */
    void handlerEvent(Channel channel, Request req) throws RemotingException {
        if (req.getData() != null && req.getData().equals(Request.READONLY_EVENT)) {
            channel.setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        }
    }

    /**
     * Processes a request and returns a response.
     * 
     * This method handles the following cases:
     * 1. Broken requests - returns BAD_REQUEST response with error details
     * 2. Normal requests - delegates to handler.reply() and returns OK response
     * 3. Exception cases - returns SERVICE_ERROR response with error details
     * 
     * The response always contains the same id and version as the request for correlation.
     *
     * @param channel The exchange channel to handle the request on
     * @param req The request to process
     * @return A response containing either the result or error information
     * @throws RemotingException If any network-related error occurs
     */
    Response handleRequest(ExchangeChannel channel, Request req) throws RemotingException {
        Response res = new Response(req.getId(), req.getVersion());
        if (req.isBroken()) {
            Object data = req.getData();

            String msg;
            if (data == null) msg = null;
            else if (data instanceof Throwable) msg = StringUtils.toString((Throwable) data);
            else msg = data.toString();
            res.setErrorMessage("Fail to decode request due to: " + msg);
            res.setStatus(Response.BAD_REQUEST);

            return res;
        }
        // find handler by message class.
        Object msg = req.getData();
        try {
            // handle data.
            Object result = handler.reply(channel, msg);
            res.setStatus(Response.OK);
            res.setResult(result);
        } catch (Throwable e) {
            res.setStatus(Response.SERVICE_ERROR);
            res.setErrorMessage(StringUtils.toString(e));
        }
        return res;
    }

    @Override
    public void connected(Channel channel) throws RemotingException {
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.connected(exchangeChannel);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.disconnected(exchangeChannel);
        } finally {
            DefaultFuture.closeChannel(channel);
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        Throwable exception = null;
        try {
            channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
            ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
            try {
                handler.sent(exchangeChannel, message);
            } finally {
                HeaderExchangeChannel.removeChannelIfDisconnected(channel);
            }
        } catch (Throwable t) {
            exception = t;
        }
        if (message instanceof Request) {
            Request request = (Request) message;
            DefaultFuture.sent(channel, request);
        }
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else if (exception instanceof RemotingException) {
                throw (RemotingException) exception;
            } else {
                throw new RemotingException(channel.getLocalAddress(), channel.getRemoteAddress(),
                        exception.getMessage(), exception);
            }
        }
    }

    /**
     * Handles messages received from the channel.
     * 
     * This method processes different types of messages:
     * 1. Request message:
     *    - Event requests (e.g. readonly event)
     *    - Two-way requests (requires response)
     *    - One-way requests (no response required)
     * 2. Response message:
     *    - Handled by DefaultFuture for async completion
     * 3. String message:
     *    - Treated as telnet commands on server side
     *    - Not supported on client side
     * 4. Other types:
     *    - Delegated directly to the underlying handler
     * 
     * The method updates the read timestamp for heartbeat purposes and ensures
     * proper channel cleanup after message processing.
     *
     * @param channel The network channel that received the message
     * @param message The received message, can be Request/Response/String/other types
     * @throws RemotingException If any error occurs during message handling
     */
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        // 更新最后读取时间戳，用于心跳检测
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        // 将普通Channel包装成ExchangeChannel
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            // 处理请求消息
            if (message instanceof Request) {
                Request request = (Request) message;
                if (request.isEvent()) {
                    // 处理事件请求，例如readonly事件
                    handlerEvent(channel, request);
                } else {
                    if (request.isTwoWay()) {
                        // 处理双向请求，需要返回响应
                        // 调用请求处理器处理请求，得到响应
                        Response response = handleRequest(exchangeChannel, request);
                        // 发送响应消息
                        channel.send(response);
                    } else {
                        // 处理单向请求，不需要返回响应
                        // 直接传递请求数据给handler处理
                        handler.received(exchangeChannel, request.getData());
                    }
                }
            }
            // 处理响应消息
            else if (message instanceof Response) {
                // 处理响应，主要是通知等待的线程
                handleResponse(channel, (Response) message);
            }
            // 处理字符串消息，通常是telnet命令
            else if (message instanceof String) {
                if (isClientSide(channel)) {
                    // 客户端不支持处理字符串消息
                    Exception e = new Exception("Dubbo client can not supported string message: " + message + " in channel: " + channel + ", url: " + channel.getUrl());
                    logger.error(e.getMessage(), e);
                } else {
                    // 服务端将字符串消息作为telnet命令处理
                    String echo = handler.telnet(channel, (String) message);
                    // 如果有返回值，发送回去
                    if (echo != null && echo.length() > 0) {
                        channel.send(echo);
                    }
                }
            }
            // 处理其他类型的消息
            else {
                // 直接交给handler处理
                handler.received(exchangeChannel, message);
            }
        } finally {
            // 如果通道已断开连接，则移除通道
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        if (exception instanceof ExecutionException) {
            ExecutionException e = (ExecutionException) exception;
            Object msg = e.getRequest();
            if (msg instanceof Request) {
                Request req = (Request) msg;
                if (req.isTwoWay() && !req.isHeartbeat()) {
                    Response res = new Response(req.getId(), req.getVersion());
                    res.setStatus(Response.SERVER_ERROR);
                    res.setErrorMessage(StringUtils.toString(e));
                    channel.send(res);
                    return;
                }
            }
        }
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.caught(exchangeChannel, exception);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }
}
