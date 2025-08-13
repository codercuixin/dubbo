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
package com.alibaba.dubbo.rpc.cluster.loadbalance;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.support.RpcUtils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 一致性哈希负载均衡实现，保证同一参数的请求总是落到同一个Invoker上。
 * 适用于参数敏感的场景，如缓存服务等。
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {

    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String methodName = RpcUtils.getMethodName(invocation);
        String key = invokers.get(0).getUrl().getServiceKey() + "." + methodName;
        int identityHashCode = System.identityHashCode(invokers);
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        if (selector == null || selector.identityHashCode != identityHashCode) {
            selectors.put(key, new ConsistentHashSelector<T>(invokers, methodName, identityHashCode));
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        return selector.select(invocation);
    }

    /**
     * 一致性哈希选择器，维护虚拟节点环。
     * <p>
     * 该类实现了基于一致性哈希算法的Invoker选择机制，
     * 通过构建虚拟节点环，将服务提供者（Invoker）映射到哈希环上，
     * 并根据调用参数的一致性哈希值选择最近的Invoker，保证同一参数的请求总是路由到同一个Invoker。
     * <ul>
     *   <li>virtualInvokers：虚拟节点哈希环，key为哈希值，value为Invoker</li>
     *   <li>replicaNumber：每个Invoker的虚拟节点数，提升哈希分布均匀性</li>
     *   <li>argumentIndex：用于参与哈希的参数下标数组，支持多参数拼接</li>
     *   <li>identityHashCode：Invoker列表的唯一标识，用于检测Invoker变更</li>
     * </ul>
     * <b>一致性哈希原理：</b>
     * <ol>
     *   <li>为每个Invoker生成多个虚拟节点，均匀分布在哈希环上</li>
     *   <li>将调用参数拼接后计算哈希值，定位到环上的某个点</li>
     *   <li>顺时针查找第一个大于等于该哈希值的虚拟节点，选中其对应的Invoker</li>
     *   <li>Invoker变更时，只有少量请求会重新分配，保证请求分布的稳定性</li>
     * </ol>
     * <b>典型应用：</b> 适用于缓存、会话等参数敏感且需要请求一致性的场景。
     */
    private static final class ConsistentHashSelector<T> {

        private final TreeMap<Long, Invoker<T>> virtualInvokers;

        private final int replicaNumber;

        private final int identityHashCode;

        private final int[] argumentIndex;

        /**
         * 构造方法，初始化虚拟节点环。
         * @param invokers 服务提供者列表
         * @param methodName 方法名
         * @param identityHashCode invokers的标识哈希
         * <p>
         * 步骤：
         * 1. 读取虚拟节点数（hash.nodes）和参与哈希的参数下标（hash.arguments）
         * 2. 为每个Invoker生成 replicaNumber 个虚拟节点，分布在哈希环上
         * 3. 虚拟节点的哈希值通过对地址+编号做MD5后分段取值获得
         */
        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            this.identityHashCode = identityHashCode;
            URL url = invokers.get(0).getUrl();
            this.replicaNumber = url.getMethodParameter(methodName, "hash.nodes", 160);
            String[] index = Constants.COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, "hash.arguments", "0"));
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            for (Invoker<T> invoker : invokers) {
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber / 4; i++) {
                    byte[] digest = md5(address + i);
                    // 将MD5摘要分为4段，每段4字节，转换为long型
                    for (int h = 0; h < 4; h++) {
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        /**
         * 根据调用参数选择Invoker。
         * <p>
         * 步骤：
         * 1. 取出指定下标的参数，拼接为字符串
         * 2. 计算MD5摘要，取第一个分段哈希值
         * 3. 在虚拟节点环上顺时针查找最近的Invoker
         * @param invocation 调用信息
         * @return 选中的Invoker
         */
        public Invoker<T> select(Invocation invocation) {
            String key = toKey(invocation.getArguments());
            byte[] digest = md5(key);
            return selectForKey(hash(digest, 0));
        }

        /**
         * 将参数拼接为哈希key。
         * <p>
         * 只拼接配置中指定下标的参数，支持多参数场景。
         * @param args 调用参数
         * @return 拼接后的字符串
         */
        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        /**
         * 根据哈希值选择最近的Invoker。
         * <p>
         * 若哈希值大于环上所有节点，则回到第一个节点，实现环形查找。
         * @param hash 参数哈希值
         * @return 最近的Invoker
         */
        private Invoker<T> selectForKey(long hash) {
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.tailMap(hash, true).firstEntry();
            if (entry == null) {
                entry = virtualInvokers.firstEntry();
            }
            return entry.getValue();
        }

        /**
         * 计算哈希值。
         * md5 返回 16 字节，刚好可以分为 4 组，每组 4 字节。
         * <p>
         * 从MD5摘要中按4字节一组取出，转为long型，提升分布均匀性。
         * @param digest MD5摘要
         * @param number 分段编号
         * @return 哈希值
         */
        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }

        /**
         * 计算MD5摘要。
         * <p>
         * 用于生成虚拟节点和参数哈希。
         * @param value 输入字符串
         * @return MD5摘要
         */
        private byte[] md5(String value) {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.reset();
            byte[] bytes;
            try {
                bytes = value.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.update(bytes);
            return md5.digest();
        }

    }

}
