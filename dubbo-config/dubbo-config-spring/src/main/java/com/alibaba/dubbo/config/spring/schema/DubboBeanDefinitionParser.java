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
package com.alibaba.dubbo.config.spring.schema;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.ArgumentConfig;
import com.alibaba.dubbo.config.ConsumerConfig;
import com.alibaba.dubbo.config.MethodConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ProviderConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.spring.ReferenceBean;
import com.alibaba.dubbo.config.spring.ServiceBean;
import com.alibaba.dubbo.rpc.Protocol;

import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * DubboBeanDefinitionParser is responsible for parsing Dubbo's custom XML elements into Spring bean definitions.
 * It handles various Dubbo configuration elements like <dubbo:service>, <dubbo:reference>, <dubbo:protocol>, etc.
 * This parser converts XML configuration into corresponding Dubbo configuration bean objects.
 *
 * @export
 */
public class DubboBeanDefinitionParser implements BeanDefinitionParser {

    private static final Logger logger = LoggerFactory.getLogger(DubboBeanDefinitionParser.class);
    // Pattern for validating group and version format
    private static final Pattern GROUP_AND_VERION = Pattern.compile("^[\\-.0-9_a-zA-Z]+(\\:[\\-.0-9_a-zA-Z]+)?$");
    // The class type of bean to be created
    private final Class<?> beanClass;
    // Whether the bean is required
    private final boolean required;

    public DubboBeanDefinitionParser(Class<?> beanClass, boolean required) {
        this.beanClass = beanClass;
        this.required = required;
    }

    /**
     * Main parsing method that converts XML element to BeanDefinition
     *
     * @param element XML element to parse
     * @param parserContext Current parser context
     * @param beanClass The class of bean to create
     * @param required Whether this bean is required
     * @return The parsed BeanDefinition
     */
    @SuppressWarnings("unchecked")
    private static BeanDefinition parse(Element element, ParserContext parserContext, Class<?> beanClass, boolean required) {
        // Create root bean definition
        RootBeanDefinition beanDefinition = new RootBeanDefinition();
        beanDefinition.setBeanClass(beanClass);
        beanDefinition.setLazyInit(false);

        // Handle ID attribute
        String id = element.getAttribute("id");
        if ((id == null || id.length() == 0) && required) {
            // Generate bean name if id is not provided
            String generatedBeanName = element.getAttribute("name");
            if (generatedBeanName == null || generatedBeanName.length() == 0) {
                // For protocol config, use "dubbo" as default name
                if (ProtocolConfig.class.equals(beanClass)) {
                    generatedBeanName = "dubbo";
                } else {
                    // For other configs, use interface name
                    generatedBeanName = element.getAttribute("interface");
                }
            }
            // If still no name, use class name
            if (generatedBeanName == null || generatedBeanName.length() == 0) {
                generatedBeanName = beanClass.getName();
            }
            id = generatedBeanName;
            // Ensure unique bean id
            int counter = 2;
            while (parserContext.getRegistry().containsBeanDefinition(id)) {
                id = generatedBeanName + (counter++);
            }
        }

        // Register bean definition if id is provided
        if (id != null && id.length() > 0) {
            if (parserContext.getRegistry().containsBeanDefinition(id)) {
                throw new IllegalStateException("Duplicate spring bean id " + id);
            }
            parserContext.getRegistry().registerBeanDefinition(id, beanDefinition);
            beanDefinition.getPropertyValues().addPropertyValue("id", id);
        }

        // Special handling for ProtocolConfig
        if (ProtocolConfig.class.equals(beanClass)) {
            // Update existing bean definitions that reference this protocol
            for (String name : parserContext.getRegistry().getBeanDefinitionNames()) {
                BeanDefinition definition = parserContext.getRegistry().getBeanDefinition(name);
                PropertyValue property = definition.getPropertyValues().getPropertyValue("protocol");
                if (property != null) {
                    Object value = property.getValue();
                    if (value instanceof ProtocolConfig && id.equals(((ProtocolConfig) value).getName())) {
                        definition.getPropertyValues().addPropertyValue("protocol", new RuntimeBeanReference(id));
                    }
                }
            }
        }
        // Handle ServiceBean class
        else if (ServiceBean.class.equals(beanClass)) {
            // Parse implementation class
            String className = element.getAttribute("class");
            if (className != null && className.length() > 0) {
                RootBeanDefinition classDefinition = new RootBeanDefinition();
                classDefinition.setBeanClass(ReflectUtils.forName(className));
                classDefinition.setLazyInit(false);
                parseProperties(element.getChildNodes(), classDefinition);
                beanDefinition.getPropertyValues().addPropertyValue("ref", new BeanDefinitionHolder(classDefinition, id + "Impl"));
            }
        }
        // Handle ProviderConfig
        else if (ProviderConfig.class.equals(beanClass)) {
            parseNested(element, parserContext, ServiceBean.class, true, "service", "provider", id, beanDefinition);
        }
        // Handle ConsumerConfig
        else if (ConsumerConfig.class.equals(beanClass)) {
            parseNested(element, parserContext, ReferenceBean.class, false, "reference", "consumer", id, beanDefinition);
        }

        // Parse properties using reflection
        Set<String> props = new HashSet<String>();
        ManagedMap parameters = null;
        // Iterate through all setter methods
        for (Method setter : beanClass.getMethods()) {
            String name = setter.getName();
            // Check if method is a valid setter
            if (name.length() > 3 && name.startsWith("set")
                    && Modifier.isPublic(setter.getModifiers())
                    && setter.getParameterTypes().length == 1) {
                Class<?> type = setter.getParameterTypes()[0];
                // Convert setter name to property name (e.g., setName -> name)
                String propertyName = name.substring(3, 4).toLowerCase() + name.substring(4);
                String property = StringUtils.camelToSplitName(propertyName, "-");
                props.add(property);
                
                // Find corresponding getter method
                Method getter = null;
                try {
                    getter = beanClass.getMethod("get" + name.substring(3), new Class<?>[0]);
                } catch (NoSuchMethodException e) {
                    try {
                        // Try "is" prefix for boolean properties
                        getter = beanClass.getMethod("is" + name.substring(3), new Class<?>[0]);
                    } catch (NoSuchMethodException e2) {
                    }
                }
                
                // Skip if getter is not properly defined
                if (getter == null
                        || !Modifier.isPublic(getter.getModifiers())
                        || !type.equals(getter.getReturnType())) {
                    continue;
                }
                
                // Handle special properties
                if ("parameters".equals(property)) {
                    // Parse <parameter> tags
                    parameters = parseParameters(element.getChildNodes(), beanDefinition);
                } else if ("methods".equals(property)) {
                    // Parse <method> tags
                    parseMethods(id, element.getChildNodes(), beanDefinition, parserContext);
                } else if ("arguments".equals(property)) {
                    // Parse <argument> tags
                    parseArguments(id, element.getChildNodes(), beanDefinition, parserContext);
                } else {
                    // Handle normal properties
                    String value = element.getAttribute(property);
                    if (value != null) {
                        value = value.trim();
                        if (value.length() > 0) {
                            // Handle registry property
                            if ("registry".equals(property) && RegistryConfig.NO_AVAILABLE.equalsIgnoreCase(value)) {
                                // Create empty registry config for "N/A" value
                                RegistryConfig registryConfig = new RegistryConfig();
                                registryConfig.setAddress(RegistryConfig.NO_AVAILABLE);
                                beanDefinition.getPropertyValues().addPropertyValue(property, registryConfig);
                            } else if ("registry".equals(property) && value.indexOf(',') != -1) {
                                // Handle multiple registries
                                parseMultiRef("registries", value, beanDefinition, parserContext);
                            } else if ("provider".equals(property) && value.indexOf(',') != -1) {
                                // Handle multiple providers
                                parseMultiRef("providers", value, beanDefinition, parserContext);
                            } else if ("protocol".equals(property) && value.indexOf(',') != -1) {
                                // Handle multiple protocols
                                parseMultiRef("protocols", value, beanDefinition, parserContext);
                            } else {
                                Object reference;
                                if (isPrimitive(type)) {
                                    // Handle primitive types and special default values
                                    if ("async".equals(property) && "false".equals(value)
                                            || "timeout".equals(property) && "0".equals(value)
                                            || "delay".equals(property) && "0".equals(value)
                                            || "version".equals(property) && "0.0.0".equals(value)
                                            || "stat".equals(property) && "-1".equals(value)
                                            || "reliable".equals(property) && "false".equals(value)) {
                                        // Skip default values for backward compatibility
                                        value = null;
                                    }
                                    reference = value;
                                } else if ("protocol".equals(property)
                                        && ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(value)
                                        && (!parserContext.getRegistry().containsBeanDefinition(value)
                                        || !ProtocolConfig.class.getName().equals(parserContext.getRegistry().getBeanDefinition(value).getBeanClassName()))) {
                                    // Handle protocol configuration
                                    if ("dubbo:provider".equals(element.getTagName())) {
                                        logger.warn("Recommended replace <dubbo:provider protocol=\"" + value + "\" ... /> to <dubbo:protocol name=\"" + value + "\" ... />");
                                    }
                                    // Create protocol config for backward compatibility
                                    ProtocolConfig protocol = new ProtocolConfig();
                                    protocol.setName(value);
                                    reference = protocol;
                                } else if ("onreturn".equals(property)) {
                                    // Handle method return callback
                                    int index = value.lastIndexOf(".");
                                    String returnRef = value.substring(0, index);
                                    String returnMethod = value.substring(index + 1);
                                    reference = new RuntimeBeanReference(returnRef);
                                    beanDefinition.getPropertyValues().addPropertyValue("onreturnMethod", returnMethod);
                                } else if ("onthrow".equals(property)) {
                                    // Handle method exception callback
                                    int index = value.lastIndexOf(".");
                                    String throwRef = value.substring(0, index);
                                    String throwMethod = value.substring(index + 1);
                                    reference = new RuntimeBeanReference(throwRef);
                                    beanDefinition.getPropertyValues().addPropertyValue("onthrowMethod", throwMethod);
                                } else if ("oninvoke".equals(property)) {
                                    // Handle method invocation callback
                                    int index = value.lastIndexOf(".");
                                    String invokeRef = value.substring(0, index);
                                    String invokeRefMethod = value.substring(index + 1);
                                    reference = new RuntimeBeanReference(invokeRef);
                                    beanDefinition.getPropertyValues().addPropertyValue("oninvokeMethod", invokeRefMethod);
                                } else {
                                    // Handle reference to other beans
                                    if ("ref".equals(property) && parserContext.getRegistry().containsBeanDefinition(value)) {
                                        BeanDefinition refBean = parserContext.getRegistry().getBeanDefinition(value);
                                        if (!refBean.isSingleton()) {
                                            throw new IllegalStateException("The exported service ref " + value + " must be singleton! Please set the " + value + " bean scope to singleton, eg: <bean id=\"" + value + "\" scope=\"singleton\" ...>");
                                        }
                                    }
                                    reference = new RuntimeBeanReference(value);
                                }
                                beanDefinition.getPropertyValues().addPropertyValue(propertyName, reference);
                            }
                        }
                    }
                }
            }
        }

        // Parse remaining attributes as parameters
        NamedNodeMap attributes = element.getAttributes();
        int len = attributes.getLength();
        for (int i = 0; i < len; i++) {
            Node node = attributes.item(i);
            String name = node.getLocalName();
            // Add attributes that are not already handled as properties
            if (!props.contains(name)) {
                if (parameters == null) {
                    parameters = new ManagedMap();
                }
                String value = node.getNodeValue();
                parameters.put(name, new TypedStringValue(value, String.class));
            }
        }
        if (parameters != null) {
            beanDefinition.getPropertyValues().addPropertyValue("parameters", parameters);
        }
        return beanDefinition;
    }

    /**
     * Checks if a class is a primitive type or its wrapper, or other basic types
     * like String, Date, and Class
     */
    private static boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() || cls == Boolean.class || cls == Byte.class
                || cls == Character.class || cls == Short.class || cls == Integer.class
                || cls == Long.class || cls == Float.class || cls == Double.class
                || cls == String.class || cls == Date.class || cls == Class.class;
    }

    /**
     * Parses comma-separated values into a list of RuntimeBeanReferences
     *
     * @param property The property name to set in the bean definition
     * @param value The comma-separated string of bean references
     * @param beanDefinition The bean definition to update
     * @param parserContext The current parser context
     */
    @SuppressWarnings("unchecked")
    private static void parseMultiRef(String property, String value, RootBeanDefinition beanDefinition,
                                      ParserContext parserContext) {
        String[] values = value.split("\\s*[,]+\\s*");
        ManagedList list = null;
        for (int i = 0; i < values.length; i++) {
            String v = values[i];
            if (v != null && v.length() > 0) {
                if (list == null) {
                    list = new ManagedList();
                }
                list.add(new RuntimeBeanReference(v));
            }
        }
        beanDefinition.getPropertyValues().addPropertyValue(property, list);
    }

    /**
     * Parses nested elements in provider/consumer configurations
     *
     * @param element The parent element
     * @param parserContext The current parser context
     * @param beanClass The class of beans to create from nested elements
     * @param required Whether the nested elements are required
     * @param tag The expected tag name of nested elements
     * @param property The property name to set in the parent bean
     * @param ref The reference to set in nested beans
     * @param beanDefinition The parent bean definition to update
     */
    private static void parseNested(Element element, ParserContext parserContext, Class<?> beanClass, boolean required, String tag, String property, String ref, BeanDefinition beanDefinition) {
        NodeList nodeList = element.getChildNodes();
        if (nodeList != null && nodeList.getLength() > 0) {
            boolean first = true;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    if (tag.equals(node.getNodeName())
                            || tag.equals(node.getLocalName())) {
                        if (first) {
                            first = false;
                            // Set default=false when nested elements exist
                            String isDefault = element.getAttribute("default");
                            if (isDefault == null || isDefault.length() == 0) {
                                beanDefinition.getPropertyValues().addPropertyValue("default", "false");
                            }
                        }
                        // Parse nested element and set reference
                        BeanDefinition subDefinition = parse((Element) node, parserContext, beanClass, required);
                        if (subDefinition != null && ref != null && ref.length() > 0) {
                            subDefinition.getPropertyValues().addPropertyValue(property, new RuntimeBeanReference(ref));
                        }
                    }
                }
            }
        }
    }

    /**
     * Parses property elements in bean definitions
     * Supports both value and reference properties
     *
     * @param nodeList List of child nodes to parse
     * @param beanDefinition The bean definition to update
     */
    private static void parseProperties(NodeList nodeList, RootBeanDefinition beanDefinition) {
        if (nodeList != null && nodeList.getLength() > 0) {
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    if ("property".equals(node.getNodeName())
                            || "property".equals(node.getLocalName())) {
                        String name = ((Element) node).getAttribute("name");
                        if (name != null && name.length() > 0) {
                            String value = ((Element) node).getAttribute("value");
                            String ref = ((Element) node).getAttribute("ref");
                            if (value != null && value.length() > 0) {
                                // Set property value directly
                                beanDefinition.getPropertyValues().addPropertyValue(name, value);
                            } else if (ref != null && ref.length() > 0) {
                                // Set reference to another bean
                                beanDefinition.getPropertyValues().addPropertyValue(name, new RuntimeBeanReference(ref));
                            } else {
                                throw new UnsupportedOperationException("Unsupported <property name=\"" + name + "\"> sub tag, Only supported <property name=\"" + name + "\" ref=\"...\" /> or <property name=\"" + name + "\" value=\"...\" />");
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Parses parameter elements into a managed map
     * Supports hiding sensitive parameters with "hide" attribute
     *
     * @param nodeList List of child nodes to parse
     * @param beanDefinition The bean definition to update
     * @return A map of parsed parameters
     */
    @SuppressWarnings("unchecked")
    private static ManagedMap parseParameters(NodeList nodeList, RootBeanDefinition beanDefinition) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedMap parameters = null;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    if ("parameter".equals(node.getNodeName())
                            || "parameter".equals(node.getLocalName())) {
                        if (parameters == null) {
                            parameters = new ManagedMap();
                        }
                        String key = ((Element) node).getAttribute("key");
                        String value = ((Element) node).getAttribute("value");
                        boolean hide = "true".equals(((Element) node).getAttribute("hide"));
                        if (hide) {
                            // Prefix key with Constants.HIDE_KEY_PREFIX for sensitive parameters
                            key = Constants.HIDE_KEY_PREFIX + key;
                        }
                        parameters.put(key, new TypedStringValue(value, String.class));
                    }
                }
            }
            return parameters;
        }
        return null;
    }

    /**
     * Parses method elements in service/reference configurations
     * Creates MethodConfig beans for each method element
     *
     * @param id Base id for generated method beans
     * @param nodeList List of child nodes to parse
     * @param beanDefinition The bean definition to update
     * @param parserContext The current parser context
     */
    @SuppressWarnings("unchecked")
    private static void parseMethods(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                     ParserContext parserContext) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedList methods = null;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    Element element = (Element) node;
                    if ("method".equals(node.getNodeName()) || "method".equals(node.getLocalName())) {
                        String methodName = element.getAttribute("name");
                        if (methodName == null || methodName.length() == 0) {
                            throw new IllegalStateException("<dubbo:method> name attribute == null");
                        }
                        if (methods == null) {
                            methods = new ManagedList();
                        }
                        // Parse method element into MethodConfig bean
                        BeanDefinition methodBeanDefinition = parse(((Element) node),
                                parserContext, MethodConfig.class, false);
                        String name = id + "." + methodName;
                        BeanDefinitionHolder methodBeanDefinitionHolder = new BeanDefinitionHolder(
                                methodBeanDefinition, name);
                        methods.add(methodBeanDefinitionHolder);
                    }
                }
            }
            if (methods != null) {
                beanDefinition.getPropertyValues().addPropertyValue("methods", methods);
            }
        }
    }

    /**
     * Parses argument elements in method configurations
     * Creates ArgumentConfig beans for each argument element
     *
     * @param id Base id for generated argument beans
     * @param nodeList List of child nodes to parse
     * @param beanDefinition The bean definition to update
     * @param parserContext The current parser context
     */
    @SuppressWarnings("unchecked")
    private static void parseArguments(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                       ParserContext parserContext) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedList arguments = null;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    Element element = (Element) node;
                    if ("argument".equals(node.getNodeName()) || "argument".equals(node.getLocalName())) {
                        String argumentIndex = element.getAttribute("index");
                        if (arguments == null) {
                            arguments = new ManagedList();
                        }
                        // Parse argument element into ArgumentConfig bean
                        BeanDefinition argumentBeanDefinition = parse(((Element) node),
                                parserContext, ArgumentConfig.class, false);
                        String name = id + "." + argumentIndex;
                        BeanDefinitionHolder argumentBeanDefinitionHolder = new BeanDefinitionHolder(
                                argumentBeanDefinition, name);
                        arguments.add(argumentBeanDefinitionHolder);
                    }
                }
            }
            if (arguments != null) {
                beanDefinition.getPropertyValues().addPropertyValue("arguments", arguments);
            }
        }
    }

    @Override
    public BeanDefinition parse(Element element, ParserContext parserContext) {
        return parse(element, parserContext, beanClass, required);
    }

}
