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
package com.alibaba.dubbo.rpc.cluster.router.condition;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Router;
import com.alibaba.dubbo.rpc.cluster.router.AbstractRouter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 基于条件表达式的路由器
 * 
 * 该路由器基于条件表达式进行服务路由，规则如下：
 * 1. 格式为：[whenCondition =>] thenCondition
 * 2. whenCondition和thenCondition都是可选的
 * 3. 条件表达式格式：key1=value1,value2 & key2!=value3 & key3
 * 4. 条件值可以使用逗号分隔，表示"或"的关系
 * 5. 支持匹配和不匹配两种规则（= 和 !=）
 * 
 * 示例：
 * host = 10.20.153.10,10.20.153.11 => host = 10.20.153.10
 * 表示：当消费者的host是10.20.153.10或10.20.153.11时，只调用10.20.153.10上的提供者
 */
public class ConditionRouter extends AbstractRouter {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(ConditionRouter.class);

    /**
     * 路由规则的默认优先级
     */
    private static final int DEFAULT_PRIORITY = 2;

    /**
     * 路由规则的解析正则表达式
     * 用于解析条件表达式中的操作符和值
     */
    private static Pattern ROUTE_PATTERN = Pattern.compile("([&!=,]*)\\s*([^&!=,\\s]+)");

    /**
     * 当路由结果为空时，是否强制执行
     * true: 直接返回空结果
     * false: 返回所有服务提供者
     */
    private final boolean force;

    /**
     * 条件路由规则的前置条件
     * 当前置条件满足时，才会进行路由规则的匹配
     */
    private final Map<String, MatchPair> whenCondition;

    /**
     * 条件路由规则的后置条件
     * 当前置条件满足时，使用后置条件进行服务提供者的过滤
     */
    private final Map<String, MatchPair> thenCondition;

    public ConditionRouter(URL url) {
        this.url = url;
        this.priority = url.getParameter(Constants.PRIORITY_KEY, DEFAULT_PRIORITY);
        this.force = url.getParameter(Constants.FORCE_KEY, false);
        try {
            String rule = url.getParameterAndDecoded(Constants.RULE_KEY);
            if (rule == null || rule.trim().length() == 0) {
                throw new IllegalArgumentException("Illegal route rule!");
            }
            rule = rule.replace("consumer.", "").replace("provider.", "");
            int i = rule.indexOf("=>");
            String whenRule = i < 0 ? null : rule.substring(0, i).trim();
            String thenRule = i < 0 ? rule.trim() : rule.substring(i + 2).trim();
            Map<String, MatchPair> when = StringUtils.isBlank(whenRule) || "true".equals(whenRule) ? new HashMap<String, MatchPair>() : parseRule(whenRule);
            Map<String, MatchPair> then = StringUtils.isBlank(thenRule) || "false".equals(thenRule) ? null : parseRule(thenRule);
            // NOTE: It should be determined on the business level whether the `When condition` can be empty or not.
            this.whenCondition = when;
            this.thenCondition = then;
        } catch (ParseException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 解析路由规则字符串
     * 
     * @param rule 路由规则字符串，格式为：key1=value1,value2 & key2!=value3 & key3
     * @return 解析后的条件映射表
     * @throws ParseException 当规则格式不正确时抛出异常
     */
    private static Map<String, MatchPair> parseRule(String rule)
            throws ParseException {
        Map<String, MatchPair> condition = new HashMap<String, MatchPair>();
        if (StringUtils.isBlank(rule)) {
            return condition;
        }
        // Key-Value pair, stores both match and mismatch conditions
        MatchPair pair = null;
        // Multiple values
        Set<String> values = null;
        final Matcher matcher = ROUTE_PATTERN.matcher(rule);
        while (matcher.find()) { // Try to match one by one
            String separator = matcher.group(1);
            String content = matcher.group(2);
            // Start part of the condition expression.
            if (separator == null || separator.length() == 0) {
                pair = new MatchPair();
                condition.put(content, pair);
            }
            // The KV part of the condition expression
            else if ("&".equals(separator)) {
                if (condition.get(content) == null) {
                    pair = new MatchPair();
                    condition.put(content, pair);
                } else {
                    pair = condition.get(content);
                }
            }
            // The Value in the KV part.
            else if ("=".equals(separator)) {
                if (pair == null)
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());

                values = pair.matches;
                values.add(content);
            }
            // The Value in the KV part.
            else if ("!=".equals(separator)) {
                if (pair == null)
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());

                values = pair.mismatches;
                values.add(content);
            }
            // The Value in the KV part, if Value have more than one items.
            else if (",".equals(separator)) { // Should be seperateed by ','
                if (values == null || values.isEmpty())
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                values.add(content);
            } else {
                throw new ParseException("Illegal route rule \"" + rule
                        + "\", The error char '" + separator + "' at index "
                        + matcher.start() + " before \"" + content + "\".", matcher.start());
            }
        }
        return condition;
    }

    /**
     * 根据路由规则进行服务路由
     * 
     * 路由过程：
     * 1. 检查前置条件(whenCondition)是否满足
     * 2. 如果满足，使用后置条件(thenCondition)对服务提供者列表进行过滤
     * 3. 如果过滤后结果为空且force=false，返回原始列表
     * 
     * @param invokers 原始的服务提供者列表
     * @param url 消费者的URL
     * @param invocation 调用信息
     * @return 经过路由筛选后的服务提供者列表
     * @throws RpcException 路由过程出现异常
     */
    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation)
            throws RpcException {
        if (invokers == null || invokers.isEmpty()) {
            return invokers;
        }
        try {
            if (!matchWhen(url, invocation)) {
                return invokers;
            }
            List<Invoker<T>> result = new ArrayList<Invoker<T>>();
            if (thenCondition == null) {
                logger.warn("The current consumer in the service blacklist. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey());
                return result;
            }
            for (Invoker<T> invoker : invokers) {
                if (matchThen(invoker.getUrl(), url)) {
                    result.add(invoker);
                }
            }
            if (!result.isEmpty()) {
                return result;
            } else if (force) {
                logger.warn("The route result is empty and force execute. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey() + ", router: " + url.getParameterAndDecoded(Constants.RULE_KEY));
                return result;
            }
        } catch (Throwable t) {
            logger.error("Failed to execute condition router rule: " + getUrl() + ", invokers: " + invokers + ", cause: " + t.getMessage(), t);
        }
        return invokers;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public int compareTo(Router o) {
        if (o == null || o.getClass() != ConditionRouter.class) {
            return 1;
        }
        ConditionRouter c = (ConditionRouter) o;
        return this.priority == c.priority ? url.toFullString().compareTo(c.url.toFullString()) : (this.priority > c.priority ? 1 : -1);
    }

    /**
     * 检查前置条件是否匹配
     * 
     * @param url 待检查的URL
     * @param invocation 调用信息
     * @return 如果前置条件为空或匹配成功返回true，否则返回false
     */
    boolean matchWhen(URL url, Invocation invocation) {
        return whenCondition == null || whenCondition.isEmpty() || matchCondition(whenCondition, url, null, invocation);
    }

    /**
     * 检查后置条件是否匹配
     * 
     * @param url 待检查的URL
     * @param param 参数URL
     * @return 如果后置条件匹配成功返回true，否则返回false
     */
    private boolean matchThen(URL url, URL param) {
        return !(thenCondition == null || thenCondition.isEmpty()) && matchCondition(thenCondition, url, param, null);
    }

    /**
     * 检查URL是否与指定的条件匹配
     * 
     * @param condition 条件集合
     * @param url 待检查的URL
     * @param param 参数URL
     * @param invocation 调用信息
     * @return 如果URL满足所有条件返回true，否则返回false
     */
    private boolean matchCondition(Map<String, MatchPair> condition, URL url, URL param, Invocation invocation) {
        Map<String, String> sample = url.toMap();
        boolean result = false;
        for (Map.Entry<String, MatchPair> matchPair : condition.entrySet()) {
            String key = matchPair.getKey();
            String sampleValue;
            //get real invoked method name from invocation
            if (invocation != null && (Constants.METHOD_KEY.equals(key) || Constants.METHODS_KEY.equals(key))) {
                sampleValue = invocation.getMethodName();
            } else {
                sampleValue = sample.get(key);
                if (sampleValue == null) {
                    sampleValue = sample.get(Constants.DEFAULT_KEY_PREFIX + key);
                }
            }
            if (sampleValue != null) {
                if (!matchPair.getValue().isMatch(sampleValue, param)) {
                    return false;
                } else {
                    result = true;
                }
            } else {
                //not pass the condition
                if (!matchPair.getValue().matches.isEmpty()) {
                    return false;
                } else {
                    result = true;
                }
            }
        }
        return result;
    }

    /**
     * 条件匹配对，用于存储路由规则中的匹配条件
     * 包含需要匹配的目标值集合和需要排除的值集合
     */
    private static final class MatchPair {
        /**
         * 匹配目标集合
         * 保存路由规则中 = 操作符后的值
         * 例如：host = 1.2.3.4,5.6.7.8 中的 1.2.3.4,5.6.7.8 会被加入此集合
         * 当值匹配此集合中任意一个规则时，表示规则匹配成功
         */
        final Set<String> matches = new HashSet<String>();

        /**
         * 排除规则集合
         * 保存路由规则中 != 操作符后的值
         * 例如：host != 1.2.3.4 中的 1.2.3.4 会被加入此集合
         * 当值匹配此集合中任意一个规则时，表示规则匹配失败
         */
        final Set<String> mismatches = new HashSet<String>();

        /**
         * 检查给定的值是否满足路由规则
         * 
         * 匹配逻辑：
         * 1. 如果只有匹配目标集合(matches)有值：
         *    - 值必须至少匹配其中一个规则才返回true
         * 2. 如果只有排除规则集合(mismatches)有值：
         *    - 值不能匹配任何一个排除规则才返回true
         * 3. 如果两个集合都有值：
         *    - 优先检查排除规则，如果匹配任何一个排除规则则返回false
         *    - 然后检查匹配规则，必须至少匹配一个规则才返回true
         * 
         * @param value 待检查的值，如主机地址、方法名等
         * @param param 参数URL，用于支持条件路由中的参数化配置
         * @return 如果值满足路由规则返回true，否则返回false
         */
        private boolean isMatch(String value, URL param) {
            // 场景1: 只有匹配规则，没有排除规则
            if (!matches.isEmpty() && mismatches.isEmpty()) {
                // 必须至少匹配一个目标值才能通过
                for (String match : matches) {
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                return false;
            }

            // 场景2: 只有排除规则，没有匹配规则
            if (!mismatches.isEmpty() && matches.isEmpty()) {
                // 不能匹配任何一个排除规则
                for (String mismatch : mismatches) {
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                        return false;
                    }
                }
                return true;
            }

            // 场景3: 同时存在匹配规则和排除规则
            if (!matches.isEmpty() && !mismatches.isEmpty()) {
                // 优先检查排除规则，任何一个匹配就返回false
                for (String mismatch : mismatches) {
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                        return false;
                    }
                }
                // 再检查匹配规则，必须至少匹配一个
                for (String match : matches) {
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                return false;
            }
            return false;
        }
    }
}
