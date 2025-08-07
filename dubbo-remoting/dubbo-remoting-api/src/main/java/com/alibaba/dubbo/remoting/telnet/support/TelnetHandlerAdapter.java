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
package com.alibaba.dubbo.remoting.telnet.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.telnet.TelnetHandler;
import com.alibaba.dubbo.remoting.transport.ChannelHandlerAdapter;

/**
 * Telnet 命令处理器的适配器实现类
 * 
 * 该类主要用于处理 Dubbo 的 Telnet 命令请求，提供了以下功能：
 * 1. 命令解析：将用户输入的文本解析为命令和参数
 * 2. 命令路由：通过 SPI 机制将命令分发到对应的处理器
 * 3. 命令控制：支持命令的启用/禁用配置
 * 4. 提示符管理：处理命令行提示符的显示
 */
public class TelnetHandlerAdapter extends ChannelHandlerAdapter implements TelnetHandler {

    /**
     * Telnet 命令处理器的扩展加载器
     * 用于动态加载和管理所有的 Telnet 命令处理器实现
     */
    private final ExtensionLoader<TelnetHandler> extensionLoader = ExtensionLoader.getExtensionLoader(TelnetHandler.class);

    /**
     * 处理 Telnet 命令请求
     * 
     * @param channel 当前通信通道，包含了连接的上下文信息
     * @param message 用户输入的命令消息
     * @return 命令执行的结果字符串，包含命令输出和提示符
     * @throws RemotingException 当命令处理发生错误时抛出
     */
    @Override
    public String telnet(Channel channel, String message) throws RemotingException {
        // 获取命令提示符，如果未配置则使用默认提示符
        String prompt = channel.getUrl().getParameterAndDecoded(Constants.PROMPT_KEY, Constants.DEFAULT_PROMPT);
        
        // 检查是否需要隐藏提示符
        boolean noprompt = message.contains("--no-prompt");
        message = message.replace("--no-prompt", "");
        
        // 用于存储命令执行结果
        StringBuilder buf = new StringBuilder();
        
        // 解析命令和参数
        message = message.trim();
        String command;
        if (message.length() > 0) {
            // 查找第一个空格，用于分隔命令和参数
            int i = message.indexOf(' ');
            if (i > 0) {
                // 提取命令名称和参数
                command = message.substring(0, i).trim();
                message = message.substring(i + 1).trim();
            } else {
                // 没有参数的情况
                command = message;
                message = "";
            }
        } else {
            // 空消息的情况
            command = "";
        }
        // 处理非空命令
        if (command.length() > 0) {
            if (extensionLoader.hasExtension(command)) {
                // 检查命令是否启用
                if (commandEnabled(channel.getUrl(), command)) {
                    try {
                        // 通过SPI机制调用对应的命令处理器
                        String result = extensionLoader.getExtension(command).telnet(channel, message);
                        if (result == null) {
                            return null;
                        }
                        buf.append(result);
                    } catch (Throwable t) {
                        // 处理命令执行异常
                        buf.append(t.getMessage());
                    }
                } else {
                    // 命令被禁用的情况
                    buf.append("Command: ");
                    buf.append(command);
                    buf.append(" disabled");
                }
            } else {
                // 未知命令的情况
                buf.append("Unsupported command: ");
                buf.append(command);
            }
        }
        
        // 添加换行符
        if (buf.length() > 0) {
            buf.append("\r\n");
        }
        
        // 添加命令提示符（如果需要）
        if (prompt != null && prompt.length() > 0 && !noprompt) {
            buf.append(prompt);
        }
        
        return buf.toString();
    }

    /**
     * 检查指定的命令是否启用
     * 
     * @param url 服务URL，包含了服务配置信息
     * @param command 要检查的命令名称
     * @return 如果命令启用返回true，否则返回false
     * 
     * 规则：
     * 1. 如果没有配置支持的命令列表，默认所有命令都启用
     * 2. 如果配置了命令列表，只有列表中的命令才启用
     */
    private boolean commandEnabled(URL url, String command) {
        boolean commandEnable = false;
        String supportCommands = url.getParameter(Constants.TELNET);
        if (StringUtils.isEmpty(supportCommands)) {
            commandEnable = true;
        } else {
            String[] commands = Constants.COMMA_SPLIT_PATTERN.split(supportCommands);
            for (String c : commands) {
                if (command.equals(c)) {
                    commandEnable = true;
                    break;
                }
            }
        }
        return commandEnable;
    }

}
