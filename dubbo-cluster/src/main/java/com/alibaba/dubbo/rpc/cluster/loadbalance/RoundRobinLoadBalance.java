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

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 加权轮询负载均衡实现。
 * 支持平滑加权轮询，能根据权重动态调整分配。
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";
    
    private static int RECYCLE_PERIOD = 60000;
    
    /**
     * 内部类，记录每个Invoker的权重和当前权重。
     */
    protected static class WeightedRoundRobin {
        private int weight;
        private AtomicLong current = new AtomicLong(0);
        private long lastUpdate;
        public int getWeight() {
            return weight;
        }
        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }
        public long increaseCurrent() {
            return current.addAndGet(weight);
        }
        public void sel(int total) {
            current.addAndGet(-1 * total);
        }
        public long getLastUpdate() {
            return lastUpdate;
        }
        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

    private ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<String, ConcurrentMap<String, WeightedRoundRobin>>();
    private AtomicBoolean updateLock = new AtomicBoolean();
    
    /**
     * get invoker addr list cached for specified invocation
     * <p>
     * <b>for unit test only</b>
     * 
     * @param invokers
     * @param invocation
     * @return
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }
    
    /**
     * 选择一个Invoker，采用平滑加权轮询算法。
     * 实现思路：
     * 1. 为每个Invoker维护一个WeightedRoundRobin对象，记录其权重、当前权重和最后更新时间。
     * 2. 每次选择时，所有Invoker的current加上自身权重，选出current最大的Invoker。
     * 3. 被选中的Invoker的current减去总权重，保证下次能轮询到其它Invoker。
     * 4. 支持权重动态变化和Invoker回收。
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 生成方法唯一key（服务+方法名），用于区分不同方法的负载均衡状态
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        // 获取或初始化该方法的Invoker权重映射表
        ConcurrentMap<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map == null) {
            methodWeightMap.putIfAbsent(key, new ConcurrentHashMap<String, WeightedRoundRobin>());
            map = methodWeightMap.get(key);
        }
        int totalWeight = 0; // 所有Invoker的权重总和
        long maxCurrent = Long.MIN_VALUE; // 当前最大current值
        long now = System.currentTimeMillis(); // 当前时间戳
        Invoker<T> selectedInvoker = null; // 最终选中的Invoker
        WeightedRoundRobin selectedWRR = null; // 选中Invoker对应的权重对象
        // 遍历所有Invoker，更新current并选出最大者
        for (Invoker<T> invoker : invokers) {
            String identifyString = invoker.getUrl().toIdentityString(); // 唯一标识
            WeightedRoundRobin weightedRoundRobin = map.get(identifyString);
            int weight = getWeight(invoker, invocation); // 获取权重（支持预热）
            if (weight < 0) {
                weight = 0;
            }
            // 若首次出现则新建WeightedRoundRobin
            if (weightedRoundRobin == null) {
                weightedRoundRobin = new WeightedRoundRobin();
                weightedRoundRobin.setWeight(weight);
                map.putIfAbsent(identifyString, weightedRoundRobin);
                weightedRoundRobin = map.get(identifyString);
            }
            // 若权重发生变化则重置
            if (weight != weightedRoundRobin.getWeight()) {
                // 权重变更，重置current
                weightedRoundRobin.setWeight(weight);
            }
            // current加上自身权重
            long cur = weightedRoundRobin.increaseCurrent();
            // 更新时间戳
            weightedRoundRobin.setLastUpdate(now);
            // 记录最大current的Invoker
            if (cur > maxCurrent) {
                maxCurrent = cur;
                selectedInvoker = invoker;
                selectedWRR = weightedRoundRobin;
            }
            totalWeight += weight;
        }
        // 回收无效Invoker，防止内存泄漏
        if (!updateLock.get() && invokers.size() != map.size()) {
            if (updateLock.compareAndSet(false, true)) {
                try {
                    // 拷贝一份map，移除长时间未更新的Invoker
                    ConcurrentMap<String, WeightedRoundRobin> newMap = new ConcurrentHashMap<String, WeightedRoundRobin>();
                    newMap.putAll(map);
                    Iterator<Entry<String, WeightedRoundRobin>> it = newMap.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, WeightedRoundRobin> item = it.next();
                        if (now - item.getValue().getLastUpdate() > RECYCLE_PERIOD) {
                            it.remove();
                        }
                    }
                    methodWeightMap.put(key, newMap);
                } finally {
                    updateLock.set(false);
                }
            }
        }
        // 选中的Invoker的current减去总权重，实现平滑轮询
        if (selectedInvoker != null) {
            selectedWRR.sel(totalWeight);
            return selectedInvoker;
        }
        // 理论不会走到这里，兜底返回第一个Invoker
        return invokers.get(0);
    }

}
