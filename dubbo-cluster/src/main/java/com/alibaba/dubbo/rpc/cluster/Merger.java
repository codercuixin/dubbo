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
package com.alibaba.dubbo.rpc.cluster;

import com.alibaba.dubbo.common.extension.SPI;

/**
 * 结果合并接口。(SPI)
 * <p>
 * 用于将多个相同类型的对象合并成一个对象。在集群环境下，当需要合并多个服务提供者返回的结果时使用。
 * 常见的合并场景包括：
 * <ul>
 * <li>数组合并</li>
 * <li>集合合并</li>
 * <li>Map合并</li>
 * <li>自定义对象合并</li>
 * </ul>
 *
 * @param <T> 要合并的对象类型
 */
@SPI
public interface Merger<T> {

    /**
     * 合并多个对象为一个对象。
     * 
     * @param items 要合并的对象数组
     * @return 合并后的结果对象
     */
    T merge(T... items);

}
