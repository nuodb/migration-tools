/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.utils.xml;

import org.simpleframework.xml.strategy.Strategy;
import org.simpleframework.xml.strategy.Type;
import org.simpleframework.xml.strategy.Value;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.NodeMap;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Map;

import static com.nuodb.migrator.utils.Collections.putAll;

@SuppressWarnings({ "unchecked", "ConstantConditions" })
public class XmlHandlerStrategy implements XmlContextStrategy {

    protected XmlHandlerRegistry registry;
    protected Strategy strategy;
    protected Map context;

    public XmlHandlerStrategy(XmlHandlerRegistry registry) {
        this.registry = registry;
    }

    public XmlHandlerStrategy(XmlHandlerRegistry registry, Strategy strategy) {
        this.registry = registry;
        this.strategy = strategy;
    }

    public XmlHandlerStrategy(XmlHandlerRegistry registry, Map context) {
        this.registry = registry;
        this.context = context;
    }

    public XmlHandlerStrategy(XmlHandlerRegistry registry, Strategy strategy, Map context) {
        this.registry = registry;
        this.strategy = strategy;
        this.context = context;
    }

    @Override
    public Value read(Type type, NodeMap<InputNode> node, Map map) throws Exception {
        XmlReadContext readContext = createReadContext(map);
        Value value = null;
        if (strategy != null) {
            value = strategy.read(type, node, readContext);
        }
        return isReference(value) ? value : read(type, node.getNode(), value, readContext);
    }

    protected XmlReadContext createReadContext(Map map) {
        return map instanceof XmlReadContext ? (XmlReadContext) map
                : new XmlReadStrategyContext(putAll(map, context), this);
    }

    protected Value read(Type type, InputNode input, Value value, XmlReadContext context) throws Exception {
        Class valueType = type.getType();
        if (value != null) {
            valueType = value.getType();
        }
        XmlReadHandler reader = lookupReader(input, valueType, context);
        if (reader != null) {
            Object data = reader.read(input, valueType, context);
            if (value != null) {
                value.setValue(data);
            } else {
                value = new Reference(data);
            }
        }
        return value;
    }

    protected XmlReadHandler lookupReader(InputNode input, Class type, XmlReadContext context) throws Exception {
        return registry.lookupReader(input, type, context);
    }

    public boolean skip(Type type, Object source, OutputNode output, Map map) throws Exception {
        XmlWriteContext writeContext = createWriteContext(map);
        Class valueType = type.getType();
        if (source != null) {
            valueType = source.getClass();
        }
        XmlWriteHandler writer = lookupWriter(source, valueType, output, writeContext);
        return writer != null && writer.skip(source, valueType, output, writeContext);
    }

    @Override
    public boolean write(Type type, Object source, NodeMap<OutputNode> node, Map map) throws Exception {
        XmlWriteContext writeContext = createWriteContext(map);
        boolean complete = false;
        if (strategy != null) {
            complete = strategy.write(type, source, node, writeContext);
        }
        return complete || write(type, source, node.getNode(), writeContext);
    }

    protected XmlWriteContext createWriteContext(Map map) {
        return map instanceof XmlWriteContext ? (XmlWriteContext) map
                : new XmlWriteStrategyContext(putAll(map, context), this);
    }

    protected boolean write(Type type, Object source, OutputNode output, XmlWriteContext context) throws Exception {
        Class valueType = type.getType();
        if (source != null) {
            valueType = source.getClass();
        }
        XmlWriteHandler writer = lookupWriter(source, valueType, output, context);
        return writer != null && writer.write(source, type.getType(), output, context);
    }

    protected XmlWriteHandler lookupWriter(Object source, Class type, OutputNode output, XmlWriteContext context)
            throws Exception {
        return registry.lookupWriter(source, type, output, context);
    }

    protected boolean isReference(Value value) {
        return value != null && value.isReference();
    }

    @Override
    public Map getContext() {
        return context;
    }

    @Override
    public void setContext(Map context) {
        this.context = context;
    }

    class Reference implements Value {

        private Object data;

        public Reference(Object value) {
            this.data = value;
        }

        public Object getValue() {
            return data;
        }

        public void setValue(Object data) {
            this.data = data;
        }

        public Class getType() {
            return data.getClass();
        }

        public int getLength() {
            return 0;
        }

        public boolean isReference() {
            return true;
        }
    }
}