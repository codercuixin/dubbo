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
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.serialize.Cleanable;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import com.alibaba.dubbo.common.utils.Assert;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.Codec;
import com.alibaba.dubbo.remoting.Decodeable;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.transport.CodecSupport;
import com.alibaba.dubbo.rpc.RpcInvocation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.dubbo.common.Constants.SERIALIZATION_ID_KEY;
import static com.alibaba.dubbo.common.Constants.SERIALIZATION_SECURITY_CHECK_KEY;
import static com.alibaba.dubbo.rpc.protocol.dubbo.CallbackServiceCodec.decodeInvocationArgument;

/**
 * 可解码的RPC调用类，用于将网络传输的二进制数据解码成RPC调用对象
 * 
 * 该类的主要职责：
 * 1. 将网络传输的二进制数据解码成RPC调用对象
 * 2. 保证解码过程的线程安全（通过volatile hasDecoded）
 * 3. 处理各种异常情况
 * 4. 支持回调参数的解码
 * 5. 提供序列化安全检查机制
 * 
 * 解码的数据格式遵循Dubbo的协议规范，按照以下固定顺序读取：
 * 1. Dubbo版本
 * 2. 服务路径
 * 3. 服务版本
 * 4. 方法名
 * 5. 参数描述符
 * 6. 参数值
 * 7. 附加参数
 */
public class DecodeableRpcInvocation extends RpcInvocation implements Codec, Decodeable {

    private static final Logger log = LoggerFactory.getLogger(DecodeableRpcInvocation.class);

    /** 通信通道 */
    private Channel channel;

    /** 序列化类型 */
    private byte serializationType;

    /** 输入流 */
    private InputStream inputStream;

    /** 请求对象 */
    private Request request;

    /** 是否已经解码的标志，使用volatile保证可见性 */
    private volatile boolean hasDecoded;

    public DecodeableRpcInvocation(Channel channel, Request request, InputStream is, byte id) {
        Assert.notNull(channel, "channel == null");
        Assert.notNull(request, "request == null");
        Assert.notNull(is, "inputStream == null");
        this.channel = channel;
        this.request = request;
        this.inputStream = is;
        this.serializationType = id;
    }

    /**
     * 解码入口方法
     * 确保解码只执行一次，并处理可能的异常情况
     */
    @Override
    public void decode() throws Exception {
        if (!hasDecoded && channel != null && inputStream != null) {
            try {
                decode(channel, inputStream);
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode rpc invocation failed: " + e.getMessage(), e);
                }
                request.setBroken(true);
                request.setData(e);
            } finally {
                hasDecoded = true;
            }
        }
    }

    @Override
    public void encode(Channel channel, OutputStream output, Object message) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * 实际的解码实现方法
     * 按照Dubbo协议规范解析输入流中的数据，将其转换为RPC调用对象
     * 
     * 解码步骤：
     * 1. 创建反序列化器
     * 2. 读取基本信息（Dubbo版本、服务路径、版本、方法名）
     * 3. 执行序列化安全检查（如果启用）
     * 4. 解析方法参数
     * 5. 处理附加参数
     * 6. 处理回调参数
     * 7. 清理工作
     * 
     * @param channel 通信通道
     * @param input 输入流
     * @return 当前对象实例
     * @throws IOException 当解码过程中发生IO异常时
     */
    @Override
    public Object decode(Channel channel, InputStream input) throws IOException {
        // 1. 根据序列化类型获取反序列化器
        ObjectInput in = CodecSupport.getSerialization(channel.getUrl(), serializationType)
                .deserialize(channel.getUrl(), input);
        this.put(SERIALIZATION_ID_KEY, serializationType);

        // 2. 读取Dubbo版本信息
        String dubboVersion = in.readUTF();
        request.setVersion(dubboVersion);
        setAttachment(Constants.DUBBO_VERSION_KEY, dubboVersion);

        // 3. 读取服务路径和版本信息
        String path = in.readUTF();
        setAttachment(Constants.PATH_KEY, path);
        String version = in.readUTF();
        setAttachment(Constants.VERSION_KEY, version);

        // 4. 读取方法名
        setMethodName(in.readUTF());
        try {
            // 5. 如果开启了序列化安全检查，则进行检查
            if (Boolean.parseBoolean(System.getProperty(SERIALIZATION_SECURITY_CHECK_KEY, "false"))) {
                CodecSupport.checkSerialization(path, version, serializationType);
            }

            // 6. 解析方法参数
            Object[] args;
            Class<?>[] pts;
            // 读取参数类型描述符
            String desc = in.readUTF();
            if (desc.length() == 0) {
                // 无参方法
                pts = DubboCodec.EMPTY_CLASS_ARRAY;
                args = DubboCodec.EMPTY_OBJECT_ARRAY;
            } else {
                // 将描述符转换为参数类型数组
                pts = ReflectUtils.desc2classArray(desc);
                args = new Object[pts.length];
                // 读取每个参数值
                for (int i = 0; i < args.length; i++) {
                    try {
                        args[i] = in.readObject(pts[i]);
                    } catch (Exception e) {
                        if (log.isWarnEnabled()) {
                            log.warn("Decode argument failed: " + e.getMessage(), e);
                        }
                    }
                }
            }
            setParameterTypes(pts);

            // 7. 读取附加参数（attachments）
            Map<String, String> map = (Map<String, String>) in.readObject(Map.class);
            if (map != null && map.size() > 0) {
                Map<String, String> attachment = getAttachments();
                if (attachment == null) {
                    attachment = new HashMap<String, String>();
                }
                attachment.putAll(map);
                setAttachments(attachment);
            }

            // 8. 处理回调参数
            for (int i = 0; i < args.length; i++) {
                args[i] = decodeInvocationArgument(channel, this, pts, i, args[i]);
            }

            setArguments(args);

        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read invocation data failed.", e));
        } finally {
            // 9. 清理工作，如果输入流支持清理接口，则进行清理
            if (in instanceof Cleanable) {
                ((Cleanable) in).cleanup();
            }
        }
        return this;
    }

}
