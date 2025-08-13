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
package com.alibaba.dubbo.rpc.cluster.router;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.cluster.Router;

/**
 * 路由规则的抽象实现类
 * 
 * 该类提供了路由器的基本功能：
 * 1. 维护路由规则的URL和优先级
 * 2. 实现路由器的比较功能，用于对多个路由器进行排序
 * 3. 作为所有具体路由实现的基类
 */
public abstract class AbstractRouter implements Router {

    /**
     * 路由规则对应的URL
     */
    protected URL url;

    /**
     * 路由规则的优先级
     * 优先级越大越靠前执行，用于在多个路由规则并存时确定执行顺序
     */
    protected int priority;

    @Override
    public URL getUrl() {
        return url;
    }

    /**
     * 比较路由器的优先级
     * 
     * @param o 要比较的另一个路由器
     * @return 小于0表示优先级低，等于0表示相等，大于0表示优先级高
     */
    @Override
    public int compareTo(Router o) {
        return (this.getPriority() < o.getPriority()) ? -1 : ((this.getPriority() == o.getPriority()) ? 0 : 1);
    }

    /**
     * 获取路由器的优先级
     * 
     * @return 路由器的优先级值
     */
    public int getPriority() {
        return priority;
    }
}
