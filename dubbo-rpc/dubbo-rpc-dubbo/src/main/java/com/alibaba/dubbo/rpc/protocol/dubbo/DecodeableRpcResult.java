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

import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.serialize.Cleanable;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import com.alibaba.dubbo.common.utils.Assert;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.Codec;
import com.alibaba.dubbo.remoting.Decodeable;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.transport.CodecSupport;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.dubbo.rpc.support.RpcUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.Map;

import static com.alibaba.dubbo.common.Constants.SERIALIZATION_ID_KEY;
import static com.alibaba.dubbo.common.Constants.SERIALIZATION_SECURITY_CHECK_KEY;

public class DecodeableRpcResult extends RpcResult implements Codec, Decodeable {

    private static final Logger log = LoggerFactory.getLogger(DecodeableRpcResult.class);

    private Channel channel;

    private byte serializationType;

    private InputStream inputStream;

    private Response response;

    private Invocation invocation;

    private volatile boolean hasDecoded;

    public DecodeableRpcResult(Channel channel, Response response, InputStream is, Invocation invocation, byte id) {
        Assert.notNull(channel, "channel == null");
        Assert.notNull(response, "response == null");
        Assert.notNull(is, "inputStream == null");
        this.channel = channel;
        this.response = response;
        this.inputStream = is;
        this.invocation = invocation;
        this.serializationType = id;
    }

    @Override
    public void encode(Channel channel, OutputStream output, Object message) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * 解码RPC调用的返回结果
     * 
     * @param channel 通信通道
     * @param input 输入流
     * @return 解码后的RPC结果对象
     * @throws IOException 当读取数据失败时抛出
     */
    @Override
    public Object decode(Channel channel, InputStream input) throws IOException {
        // 根据序列化类型获取反序列化器
        ObjectInput in = CodecSupport.getSerialization(channel.getUrl(), serializationType)
                .deserialize(channel.getUrl(), input);
        
        // 读取响应标志位，用于判断响应类型
        byte flag = in.readByte();
        switch (flag) {
            case DubboCodec.RESPONSE_NULL_VALUE:
                // 响应为空值，不需要额外处理
                break;
            case DubboCodec.RESPONSE_VALUE:
                try {
                    // 获取返回值类型
                    Type[] returnType = RpcUtils.getReturnTypes(invocation);
                    // 根据返回值类型反序列化响应数据
                    setValue(returnType == null || returnType.length == 0 ? in.readObject() :
                            (returnType.length == 1 ? in.readObject((Class<?>) returnType[0])
                                    : in.readObject((Class<?>) returnType[0], returnType[1])));
                } catch (ClassNotFoundException e) {
                    throw new IOException(StringUtils.toString("Read response data failed.", e));
                }
                break;
            case DubboCodec.RESPONSE_WITH_EXCEPTION:
                try {
                    // 读取异常对象
                    Object obj = in.readObject();
                    if (obj instanceof Throwable == false)
                        throw new IOException("Response data error, expect Throwable, but get " + obj);
                    setException((Throwable) obj);
                } catch (ClassNotFoundException e) {
                    throw new IOException(StringUtils.toString("Read response data failed.", e));
                }
                break;
            case DubboCodec.RESPONSE_NULL_VALUE_WITH_ATTACHMENTS:
                try {
                    // 读取附加参数（为空值）
                    setAttachments((Map<String, String>) in.readObject(Map.class));
                } catch (ClassNotFoundException e) {
                    throw new IOException(StringUtils.toString("Read response data failed.", e));
                }
                break;
            case DubboCodec.RESPONSE_VALUE_WITH_ATTACHMENTS:
                try {
                    // 获取返回值类型
                    Type[] returnType = RpcUtils.getReturnTypes(invocation);
                    // 读取返回值和附加参数
                    setValue(returnType == null || returnType.length == 0 ? in.readObject() :
                            (returnType.length == 1 ? in.readObject((Class<?>) returnType[0])
                                    : in.readObject((Class<?>) returnType[0], returnType[1])));
                    setAttachments((Map<String, String>) in.readObject(Map.class));
                } catch (ClassNotFoundException e) {
                    throw new IOException(StringUtils.toString("Read response data failed.", e));
                }
                break;
            case DubboCodec.RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS:
                try {
                    // 读取异常对象和附加参数
                    Object obj = in.readObject();
                    if (obj instanceof Throwable == false)
                        throw new IOException("Response data error, expect Throwable, but get " + obj);
                    setException((Throwable) obj);
                    setAttachments((Map<String, String>) in.readObject(Map.class));
                } catch (ClassNotFoundException e) {
                    throw new IOException(StringUtils.toString("Read response data failed.", e));
                }
                break;
            default:
                throw new IOException("Unknown result flag, expect '0' '1' '2' '3' '4' '5', get " + flag);
        }
        // 如果输入流支持清理，则进行清理
        if (in instanceof Cleanable) {
            ((Cleanable) in).cleanup();
        }
        return this;
    }

    @Override
    public void decode() throws Exception {
        if (!hasDecoded && channel != null && inputStream != null) {
            try {
                if (Boolean.parseBoolean(System.getProperty(SERIALIZATION_SECURITY_CHECK_KEY, "false")) && invocation != null) {
                    Object serializationType_obj = invocation.get(SERIALIZATION_ID_KEY);
                    if (serializationType_obj != null) {
                        if (((Byte) serializationType_obj).compareTo(serializationType) != 0) {
                            throw new IOException("Unexpected serialization id:" + serializationType + " received from network, please check if the peer send the right id.");
                        }
                    }
                }

                decode(channel, inputStream);
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode rpc result failed: " + e.getMessage(), e);
                }
                response.setStatus(Response.CLIENT_ERROR);
                response.setErrorMessage(StringUtils.toString(e));
            } finally {
                hasDecoded = true;
            }
        }
    }

    public Invocation getInvocation() {
        return invocation;
    }
}
