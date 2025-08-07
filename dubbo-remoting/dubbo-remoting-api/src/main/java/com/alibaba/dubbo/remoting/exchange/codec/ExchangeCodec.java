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
package com.alibaba.dubbo.remoting.exchange.codec;

import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.io.Bytes;
import com.alibaba.dubbo.common.io.StreamUtils;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.serialize.Cleanable;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import com.alibaba.dubbo.common.serialize.ObjectOutput;
import com.alibaba.dubbo.common.serialize.Serialization;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;
import com.alibaba.dubbo.remoting.buffer.ChannelBufferInputStream;
import com.alibaba.dubbo.remoting.buffer.ChannelBufferOutputStream;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.support.DefaultFuture;
import com.alibaba.dubbo.remoting.telnet.codec.TelnetCodec;
import com.alibaba.dubbo.remoting.transport.CodecSupport;
import com.alibaba.dubbo.remoting.transport.ExceedPayloadLimitException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * ExchangeCodec 是 Dubbo 的通信协议编解码器，继承自 TelnetCodec。
 * ![dubbo-encoding-protocol.png](dubbo-encoding-protocol.png)
 */
public class ExchangeCodec extends TelnetCodec {

    /**
     * 协议头长度，固定为 16 字节
     */
    protected static final int HEADER_LENGTH = 16;

    /**
     * 协议魔数，固定为 0xdabb
     */
    protected static final short MAGIC = (short) 0xdabb;

    /**
     * 魔数高位字节
     */
    protected static final byte MAGIC_HIGH = Bytes.short2bytes(MAGIC)[0];

    /**
     * 魔数低位字节
     */
    protected static final byte MAGIC_LOW = Bytes.short2bytes(MAGIC)[1];

    /**
     * 消息标志位
     * 1000 0000 = 请求标志
     */
    protected static final byte FLAG_REQUEST = (byte) 0x80;

    /**
     * 消息标志位
     * 0100 0000 = 双向通信标志
     */
    protected static final byte FLAG_TWOWAY = (byte) 0x40;

    /**
     * 消息标志位
     * 0010 0000 = 事件标志（如心跳事件）
     */
    protected static final byte FLAG_EVENT = (byte) 0x20;

    /**
     * 序列化类型掩码
     * 0001 1111 = 用于获取序列化器编号
     */
    protected static final int SERIALIZATION_MASK = 0x1f;
    private static final Logger logger = LoggerFactory.getLogger(ExchangeCodec.class);

    public Short getMagicCode() {
        return MAGIC;
    }

    /**
     * 编码消息对象
     *
     * @param channel 通信通道
     * @param buffer 数据缓冲区
     * @param msg 待编码的消息对象（可能是 Request、Response 或其他类型）
     * @throws IOException 编码过程中可能出现的IO异常
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        if (msg instanceof Request) {
            encodeRequest(channel, buffer, (Request) msg);
        } else if (msg instanceof Response) {
            encodeResponse(channel, buffer, (Response) msg);
        } else {
            super.encode(channel, buffer, msg);
        }
    }

    /**
     * 解码消息数据
     *
     * @param channel 通信通道
     * @param buffer 数据缓冲区
     * @return 解码后的消息对象
     * @throws IOException 解码过程中可能出现的IO异常
     */
    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        int readable = buffer.readableBytes();
        byte[] header = new byte[Math.min(readable, HEADER_LENGTH)];
        buffer.readBytes(header);
        return decode(channel, buffer, readable, header);
    }

    /**
     * 解码消息数据的具体实现
     *
     * @param channel 通信通道
     * @param buffer 数据缓冲区
     * @param readable 可读数据长度
     * @param header 消息头数据
     * @return 解码后的消息对象
     * @throws IOException 解码过程中可能出现的IO异常
     */
    @Override
    protected Object decode(Channel channel, ChannelBuffer buffer, int readable, byte[] header) throws IOException {
        // 检查魔数，如果不匹配，可能是Telnet等其他协议的数据
        if (readable > 0 && header[0] != MAGIC_HIGH
                || readable > 1 && header[1] != MAGIC_LOW) {
            // 流中如果还有其他数据，则将数据复制到header中
            int length = header.length;
            if (header.length < readable) {
                header = Bytes.copyOf(header, readable);
                buffer.readBytes(header, length, readable - length);
            }
            // 在数据中查找魔数的起始位置
            for (int i = 1; i < header.length - 1; i++) {
                if (header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW) {
                    // 找到魔数后，调整缓冲区的读索引
                    buffer.readerIndex(buffer.readerIndex() - header.length + i);
                    // 将header截断到魔数位置
                    header = Bytes.copyOf(header, i);
                    break;
                }
            }
            // 用于解码 header 数据，比如 telnet 调用
            return super.decode(channel, buffer, readable, header);
        }

        // 检查数据是否足够读取消息头
        if (readable < HEADER_LENGTH) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // 获取消息体长度
        int len = Bytes.bytes2int(header, 12);
        // 检查消息体长度是否超过限制
        checkPayload(channel, len);

        // 检查数据是否足够读取完整消息（头部 + 消息体）
        int tt = len + HEADER_LENGTH;
        if (readable < tt) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // 创建输入流，限制读取长度为消息体长度
        ChannelBufferInputStream is = new ChannelBufferInputStream(buffer, len);

        try {
            // 解码消息体
            return decodeBody(channel, is, header);
        } finally {
            // 确保读取完所有数据，避免内存泄漏
            if (is.available() > 0) {
                try {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Skip input stream " + is.available());
                    }
                    StreamUtils.skipUnusedStream(is);
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 解码消息体
     *
     * @param channel 通信通道
     * @param is 输入流，包含消息体数据
     * @param header 消息头数据
     * @return 解码后的消息对象（Request 或 Response）
     * @throws IOException 解码过程中可能出现的IO异常
     */
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        // 获取标志位和序列化类型
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        // 获取请求ID
        long id = Bytes.bytes2long(header, 4);

        // 判断是响应还是请求
        if ((flag & FLAG_REQUEST) == 0) {
            // 解码响应消息
            Response res = new Response(id);
            // 检查是否是心跳事件
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(Response.HEARTBEAT_EVENT);
            }
            // 获取响应状态
            byte status = header[3];
            res.setStatus(status);
            try {
                // 反序列化响应数据
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                if (status == Response.OK) {
                    Object data;
                    if (res.isHeartbeat()) {
                        // 解码心跳数据
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        data = decodeHeartbeatData(channel, CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                    } else if (res.isEvent()) {
                        // 解码事件数据
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        data = decodeEventData(channel,
                                CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                    } else {
                        // 解码普通响应数据
                        data = decodeResponseData(channel, in, getRequestData(id));
                    }
                    res.setResult(data);
                } else {
                    // 如果响应状态不是OK，读取错误信息
                    res.setErrorMessage(in.readUTF());
                }
            } catch (Throwable t) {
                // 发生异常时设置客户端错误状态
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // 解码请求消息
            Request req = new Request(id);
            req.setVersion(Version.getProtocolVersion());
            // 设置是否需要响应
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            // 检查是否是心跳事件
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(Request.HEARTBEAT_EVENT);
            }
            try {
                // 反序列化请求数据
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                Object data;
                if (req.isHeartbeat()) {
                    // 解码心跳数据
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    data = decodeHeartbeatData(channel,
                            CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                } else if (req.isEvent()) {
                    // 解码事件数据
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    data = decodeEventData(channel,
                            CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                } else {
                    // 解码普通请求数据
                    data = decodeRequestData(channel, in);
                }
                req.setData(data);
            } catch (Throwable t) {
                // 请求解码失败时标记为损坏的请求
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    /**
     * 获取请求数据
     *
     * @param id 请求ID
     * @return 请求数据对象
     */
    protected Object getRequestData(long id) {
        DefaultFuture future = DefaultFuture.getFuture(id);
        if (future == null)
            return null;
        Request req = future.getRequest();
        if (req == null)
            return null;
        return req.getData();
    }

    /**
     * 编码请求消息
     *
     * @param channel 通信通道
     * @param buffer 数据缓冲区
     * @param req 请求对象
     * @throws IOException 编码过程中可能出现的IO异常
     */
    protected void encodeRequest(Channel channel, ChannelBuffer buffer, Request req) throws IOException {
        // 获取序列化器
        Serialization serialization = getSerialization(channel);

        // 创建消息头
        byte[] header = new byte[HEADER_LENGTH];
        // 设置魔数
        Bytes.short2bytes(MAGIC, header);

        // 设置请求标志和序列化类型标志
        header[2] = (byte) (FLAG_REQUEST | serialization.getContentTypeId());

        // 设置双向通信标志
        if (req.isTwoWay()) header[2] |= FLAG_TWOWAY;
        // 设置事件标志
        if (req.isEvent()) header[2] |= FLAG_EVENT;

        // 设置请求ID
        Bytes.long2bytes(req.getId(), header, 4);

        // 编码请求数据
        int savedWriteIndex = buffer.writerIndex();
        // 预留头部空间
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
        // 创建输出流
        ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);
        ObjectOutput out = serialization.serialize(channel.getUrl(), bos);

        // 根据消息类型选择编码方式
        if (req.isEvent()) {
            encodeEventData(channel, out, req.getData());
        } else {
            encodeRequestData(channel, out, req.getData(), req.getVersion());
        }

        // 刷新并清理输出流
        out.flushBuffer();
        if (out instanceof Cleanable) {
            ((Cleanable) out).cleanup();
        }
        bos.flush();
        bos.close();

        // 获取消息体长度并检查是否超限
        int len = bos.writtenBytes();
        checkPayload(channel, len);
        // 设置消息体长度
        Bytes.int2bytes(len, header, 12);

        // 写入完整消息
        buffer.writerIndex(savedWriteIndex);
        buffer.writeBytes(header); // 写入消息头
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
    }

    /**
     * 编码响应消息
     *
     * @param channel 通信通道
     * @param buffer 数据缓冲区
     * @param res 响应对象
     * @throws IOException 编码过程中可能出现的IO异常
     */
    protected void encodeResponse(Channel channel, ChannelBuffer buffer, Response res) throws IOException {
        // 保存当前写入位置
        int savedWriteIndex = buffer.writerIndex();
        try {
            // 获取序列化器
            Serialization serialization = getSerialization(channel);

            // 创建消息头
            byte[] header = new byte[HEADER_LENGTH];
            // 设置魔数
            Bytes.short2bytes(MAGIC, header);
            // 设置序列化类型标志
            header[2] = serialization.getContentTypeId();
            // 设置心跳事件标志
            if (res.isHeartbeat()) header[2] |= FLAG_EVENT;
            // 设置响应状态
            byte status = res.getStatus();
            header[3] = status;
            // 设置请求ID
            Bytes.long2bytes(res.getId(), header, 4);

            // 预留头部空间
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
            // 创建输出流
            ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);
            ObjectOutput out = serialization.serialize(channel.getUrl(), bos);

            // 根据响应状态编码响应数据或错误信息
            if (status == Response.OK) {
                if (res.isHeartbeat()) {
                    // 编码心跳数据
                    encodeHeartbeatData(channel, out, res.getResult());
                } else {
                    // 编码正常响应数据
                    encodeResponseData(channel, out, res.getResult(), res.getVersion());
                }
            } else {
                // 编码错误信息
                out.writeUTF(res.getErrorMessage());
            }

            // 刷新并清理输出流
            out.flushBuffer();
            if (out instanceof Cleanable) {
                ((Cleanable) out).cleanup();
            }
            bos.flush();
            bos.close();

            // 获取消息体长度并检查是否超限
            int len = bos.writtenBytes();
            checkPayload(channel, len);
            // 设置消息体长度
            Bytes.int2bytes(len, header, 12);

            // 写入完整消息
            buffer.writerIndex(savedWriteIndex);
            buffer.writeBytes(header); // 写入消息头
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
        } catch (Throwable t) {
            // 发生异常时清空缓冲区
            buffer.writerIndex(savedWriteIndex);

            // 对于非事件且非BAD_RESPONSE状态的响应，发送错误信息给消费者，避免消费者一直等待超时
            if (!res.isEvent() && res.getStatus() != Response.BAD_RESPONSE) {
                Response r = new Response(res.getId(), res.getVersion());
                r.setStatus(Response.BAD_RESPONSE);

                if (t instanceof ExceedPayloadLimitException) {
                    // 处理超出负载限制异常
                    logger.warn(t.getMessage(), t);
                    try {
                        r.setErrorMessage(t.getMessage());
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + t.getMessage() + ", cause: " + e.getMessage(), e);
                    }
                } else {
                    // 处理其他编码异常
                    logger.warn("Fail to encode response: " + res + ", send bad_response info instead, cause: " + t.getMessage(), t);
                    try {
                        r.setErrorMessage("Failed to send response: " + res + ", cause: " + StringUtils.toString(t));
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + res + ", cause: " + e.getMessage(), e);
                    }
                }
            }

            // 重新抛出异常
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t.getMessage(), t);
            }
        }
    }

    /**
     * 解码数据（重写父类方法）
     */
    @Override
    protected Object decodeData(ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    /**
     * 解码请求数据
     *
     * @param in 输入流
     * @return 解码后的请求数据对象
     * @throws IOException 当读取对象失败时抛出异常
     */
    protected Object decodeRequestData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    /**
     * 解码响应数据
     *
     * @param in 输入流
     * @return 解码后的响应数据对象
     * @throws IOException 当读取对象失败时抛出异常
     */
    protected Object decodeResponseData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    /**
     * 编码数据（重写父类方法）
     */
    @Override
    protected void encodeData(ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    /**
     * 编码事件数据
     *
     * @param out 输出流
     * @param data 事件数据对象
     * @throws IOException 当写入对象失败时抛出异常
     */
    private void encodeEventData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    /**
     * 编码心跳数据（已废弃）
     *
     * @deprecated 使用 {@link #encodeEventData} 代替
     */
    @Deprecated
    protected void encodeHeartbeatData(ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    /**
     * 编码请求数据
     *
     * @param out 输出流
     * @param data 请求数据对象
     * @throws IOException 当写入对象失败时抛出异常
     */
    protected void encodeRequestData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    /**
     * 编码响应数据
     *
     * @param out 输出流
     * @param data 响应数据对象
     * @throws IOException 当写入对象失败时抛出异常
     */
    protected void encodeResponseData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    @Override
    protected Object decodeData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(channel, in);
    }

    protected Object decodeEventData(Channel channel, ObjectInput in, byte[] eventPayload) throws IOException {
        try {
            int dataLen = eventPayload.length;
            int threshold = Integer.parseInt(System.getProperty("deserialization.event.size", "50"));
            if (dataLen > threshold) {
                throw new IllegalArgumentException("Event data too long, actual size " + dataLen + ", threshold " + threshold + " rejected for security consideration.");
            }
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Deprecated
    protected Object decodeHeartbeatData(Channel channel, ObjectInput in, byte[] eventPayload) throws IOException {
        return decodeEventData(channel, in, eventPayload);
    }

    protected Object decodeRequestData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in) throws IOException {
        return decodeResponseData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in, Object requestData) throws IOException {
        return decodeResponseData(channel, in);
    }

    @Override
    protected void encodeData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data);
    }

    private void encodeEventData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    @Deprecated
    protected void encodeHeartbeatData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeHeartbeatData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeResponseData(out, data);
    }


}
