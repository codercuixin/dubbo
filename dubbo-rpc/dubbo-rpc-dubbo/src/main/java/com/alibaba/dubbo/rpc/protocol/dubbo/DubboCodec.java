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
package com.alibaba.dubbo.rpc.protocol.dubbo;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.io.Bytes;
import com.alibaba.dubbo.common.io.UnsafeByteArrayInputStream;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.serialize.ObjectOutput;
import com.alibaba.dubbo.common.serialize.Serialization;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.codec.ExchangeCodec;
import com.alibaba.dubbo.remoting.transport.CodecSupport;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcInvocation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.alibaba.dubbo.rpc.protocol.dubbo.CallbackServiceCodec.encodeInvocationArgument;

/**
 * Dubbo协议编解码器，继承自ExchangeCodec，主要负责Dubbo协议的编码和解码工作。
 * 
 */
public class DubboCodec extends ExchangeCodec implements Codec2 {

    /** 协议名称 */
    public static final String NAME = "dubbo";
    
    /** Dubbo协议版本 */
    public static final String DUBBO_VERSION = Version.getProtocolVersion();
    
    /** 响应类型：异常响应 */
    public static final byte RESPONSE_WITH_EXCEPTION = 0;
    
    /** 响应类型：有返回值 */
    public static final byte RESPONSE_VALUE = 1;
    
    /** 响应类型：空返回值 */
    public static final byte RESPONSE_NULL_VALUE = 2;
    
    /** 响应类型：带附件的异常响应 */
    public static final byte RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3;
    
    /** 响应类型：带附件的有返回值 */
    public static final byte RESPONSE_VALUE_WITH_ATTACHMENTS = 4;
    
    /** 响应类型：带附件的空返回值 */
    public static final byte RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5;
    
    /** 空对象数组常量 */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    
    /** 空类数组常量 */
    public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];
    
    /** 日志对象 */
    private static final Logger log = LoggerFactory.getLogger(DubboCodec.class);

    /**
     * 解码消息体，将二进制数据转换为Request或Response对象
     * 
     * @param channel 通信通道
     * @param is 输入流，包含消息体数据
     * @param header 消息头数据，包含Magic、Flag、Status、Request ID等信息
     * @return 解码后的请求或响应对象
     * @throws IOException 当读取数据失败时抛出
     * 
     * 解码过程说明：
     * 1. 首先从header中解析出关键信息：
     *    - 序列化类型(proto)：从flag中获取序列化掩码
     *    - 请求ID(id)：用于关联请求和响应
     *    - 消息类型：通过flag判断是请求还是响应
     * 
     * 2. 响应消息解码(flag & FLAG_REQUEST == 0)：
     *    - 创建Response对象，设置请求ID
     *    - 判断是否为心跳事件
     *    - 获取响应状态码
     *    - 根据状态码处理响应内容：
     *      a) OK状态：解码响应数据（心跳、事件或RPC结果）
     *      b) 异常状态：解码错误信息
     * 
     * 3. 请求消息解码(flag & FLAG_REQUEST != 0)：
     *    - 创建Request对象，设置请求ID和版本
     *    - 设置是否需要响应(TWO_WAY)
     *    - 判断是否为心跳事件
     *    - 根据消息类型解码请求数据：
     *      a) 心跳请求：解码心跳数据
     *      b) 事件请求：解码事件数据
     *      c) 普通请求：解码RPC调用信息
     * 
     * 4. 特殊处理：
     *    - IO线程解码：通过DECODE_IN_IO_THREAD_KEY参数控制
     *    - 异常处理：捕获解码过程中的异常并记录日志
     */
    @Override
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        // 从消息头中获取标志位和序列化类型
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        // 从消息头中获取请求ID（用于请求和响应的配对）
        long id = Bytes.bytes2long(header, 4);

        // 判断是响应还是请求消息
        if ((flag & FLAG_REQUEST) == 0) {
            // ======= 解码响应消息 =======
            // 创建响应对象，并设置请求ID
            Response res = new Response(id);
            // 检查是否是事件类型消息（如心跳事件）
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(Response.HEARTBEAT_EVENT);
            }
            // 获取响应状态码
            byte status = header[3];
            res.setStatus(status);

            try {
                // 处理正常响应
                if (status == Response.OK) {
                    Object data;
                    // 1. 处理心跳响应
                    if (res.isHeartbeat()) {
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        data = decodeHeartbeatData(channel,
                                CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                    } 
                    // 2. 处理其他事件响应
                    else if (res.isEvent()) {
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        data = decodeEventData(channel,
                                CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                    } 
                    // 3. 处理普通RPC响应
                    else {
                        DecodeableRpcResult result;
                        // 判断是否在IO线程中解码
                        if (channel.getUrl().getParameter(
                                Constants.DECODE_IN_IO_THREAD_KEY,
                                Constants.DEFAULT_DECODE_IN_IO_THREAD)) {
                            // 在IO线程中直接解码
                            result = new DecodeableRpcResult(channel, res, is,
                                    (Invocation) getRequestData(id), proto);
                            result.decode();
                        } else {
                            // 将数据读取到字节数组，延迟解码
                            result = new DecodeableRpcResult(channel, res,
                                    new UnsafeByteArrayInputStream(readMessageData(is)),
                                    (Invocation) getRequestData(id), proto);
                        }
                        data = result;
                    }
                    res.setResult(data);
                } else {
                    // 处理异常响应，读取错误信息
                    res.setErrorMessage(CodecSupport.deserialize(channel.getUrl(), is, proto).readUTF());
                }
            } catch (Throwable t) {
                // 解码过程中发生异常，记录警告日志
                if (log.isWarnEnabled()) {
                    log.warn("Decode response failed: " + t.getMessage(), t);
                }
                // 设置客户端错误状态和错误信息
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // ======= 解码请求消息 =======
            // 创建请求对象，设置基本信息
            Request req = new Request(id);
            req.setVersion(Version.getProtocolVersion());
            // 设置是否需要响应（双向通信）
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            // 检查是否是事件类型消息（如心跳事件）
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(Request.HEARTBEAT_EVENT);
            }

            try {
                Object data;
                // 1. 处理心跳请求
                if (req.isHeartbeat()) {
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    data = decodeHeartbeatData(channel,
                            CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                } 
                // 2. 处理其他事件请求
                else if (req.isEvent()) {
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    data = decodeEventData(channel,
                            CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                } 
                // 3. 处理普通RPC请求
                else {
                    DecodeableRpcInvocation inv;
                    // 判断是否在IO线程中解码
                    if (channel.getUrl().getParameter(
                            Constants.DECODE_IN_IO_THREAD_KEY,
                            Constants.DEFAULT_DECODE_IN_IO_THREAD)) {
                        // 在IO线程中直接解码
                        inv = new DecodeableRpcInvocation(channel, req, is, proto);
                        inv.decode();
                    } else {
                        // 将数据读取到字节数组，延迟解码
                        inv = new DecodeableRpcInvocation(channel, req,
                                new UnsafeByteArrayInputStream(readMessageData(is)), proto);
                    }
                    data = inv;
                }
                req.setData(data);
            } catch (Throwable t) {
                // 解码过程中发生异常，记录警告日志
                if (log.isWarnEnabled()) {
                    log.warn("Decode request failed: " + t.getMessage(), t);
                }
                // 标记请求为损坏状态，并设置异常信息
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    /**
     * 读取消息体数据
     * 
     * @param is 输入流
     * @return 消息体字节数组
     * @throws IOException 当读取数据失败时抛出
     */
    private byte[] readMessageData(InputStream is) throws IOException {
        if (is.available() > 0) {
            byte[] result = new byte[is.available()];
            is.read(result);
            return result;
        }
        return new byte[]{};
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data, DUBBO_VERSION);
    }

    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(channel, out, data, DUBBO_VERSION);
    }

    /**
     * 编码请求数据
     * 
     * @param channel 通信通道
     * @param out 输出流
     * @param data RPC调用信息
     * @param version Dubbo协议版本
     * @throws IOException 当写入数据失败时抛出
     */
    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        RpcInvocation inv = (RpcInvocation) data;

        out.writeUTF(version);
        out.writeUTF(inv.getAttachment(Constants.PATH_KEY));
        out.writeUTF(inv.getAttachment(Constants.VERSION_KEY));

        out.writeUTF(inv.getMethodName());
        out.writeUTF(ReflectUtils.getDesc(inv.getParameterTypes()));
        Object[] args = inv.getArguments();
        if (args != null)
            for (int i = 0; i < args.length; i++) {
                out.writeObject(encodeInvocationArgument(channel, inv, i));
            }
        out.writeObject(inv.getAttachments());
    }

    /**
     * 编码响应数据
     * 
     * @param channel 通信通道
     * @param out 输出流
     * @param data RPC调用结果
     * @param version Dubbo协议版本
     * @throws IOException 当写入数据失败时抛出
     */
    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        Result result = (Result) data;
        // currently, the version value in Response records the version of Request
        boolean attach = Version.isSupportResponseAttatchment(version);
        Throwable th = result.getException();
        if (th == null) {
            Object ret = result.getValue();
            if (ret == null) {
                out.writeByte(attach ? RESPONSE_NULL_VALUE_WITH_ATTACHMENTS : RESPONSE_NULL_VALUE);
            } else {
                out.writeByte(attach ? RESPONSE_VALUE_WITH_ATTACHMENTS : RESPONSE_VALUE);
                out.writeObject(ret);
            }
        } else {
            out.writeByte(attach ? RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS : RESPONSE_WITH_EXCEPTION);
            out.writeObject(th);
        }

        if (attach) {
            // returns current version of Response to consumer side.
            result.getAttachments().put(Constants.DUBBO_VERSION_KEY, Version.getProtocolVersion());
            out.writeObject(result.getAttachments());
        }
    }

    /**
     * 获取请求的序列化方式
     * 
     * @param channel 通信通道
     * @param req 请求对象
     * @return 序列化实现
     */
    @Override
    protected Serialization getSerialization(Channel channel, Request req) {
        if (!(req.getData() instanceof Invocation)) {
            return super.getSerialization(channel, req);
        }
        return DubboCodecSupport.getRequestSerialization(channel.getUrl(), (Invocation) req.getData());
    }

    /**
     * 获取响应的序列化方式
     * 
     * @param channel 通信通道
     * @param res 响应对象
     * @return 序列化实现
     */
    @Override
    protected Serialization getSerialization(Channel channel, Response res) {
        if (!(res.getResult() instanceof DecodeableRpcResult)) {
            return super.getSerialization(channel, res);
        }
        return DubboCodecSupport.getResponseSerialization(channel.getUrl(), (DecodeableRpcResult) res.getResult());
    }


}
