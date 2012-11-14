/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.migration.config.xml;

import org.simpleframework.xml.strategy.Strategy;
import org.simpleframework.xml.strategy.Type;
import org.simpleframework.xml.strategy.Value;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.NodeMap;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Map;

@SuppressWarnings("unchecked")
public class XmlHandlerStrategy implements Strategy {

    protected XmlHandlerRegistry registry;
    protected Strategy strategy;

    public XmlHandlerStrategy(XmlHandlerRegistry registry) {
        this.registry = registry;
    }

    public XmlHandlerStrategy(XmlHandlerRegistry registry, Strategy strategy) {
        this.registry = registry;
        this.strategy = strategy;
    }

    @Override
    public Value read(Type type, NodeMap<InputNode> node, Map map) throws Exception {
        XmlReadContext context = getReaderContext(map);
        Value value = null;
        if (strategy != null) {
            value = strategy.read(type, node, context);
        }
        return isReference(value) ? value : read(type, node.getNode(), value, context);
    }

    protected XmlReadContext getReaderContext(Map map) {
        return map instanceof XmlReadContext ? (XmlReadContext) map : new XmlReadContextImpl(this, map);
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

    @Override
    public boolean write(Type type, Object value, NodeMap<OutputNode> node, Map map) throws Exception {
        XmlWriteContext context = getWriteContext(map);
        boolean complete = false;
        if (strategy != null) {
            complete = strategy.write(type, value, node, context);
        }
        return complete || write(type, value, node.getNode(), context);
    }

    protected XmlWriteContext getWriteContext(Map map) {
        return map instanceof XmlWriteContext ? (XmlWriteContext) map : new XmlWriteContextImpl(this, map);
    }


    protected boolean write(Type type, Object value, OutputNode output, XmlWriteContext context) throws Exception {
        Class valueType = type.getType();
        if (value != null) {
            valueType = value.getClass();
        }
        XmlWriteHandler writer = lookupWriter(value, valueType, output, context);
        if (writer != null) {
            writer.write(value, type.getType(), output, context);
            return true;
        }
        return false;
    }

    protected XmlReadHandler lookupReader(InputNode input, Class type, XmlReadContext context) throws Exception {
        return registry.lookupReader(input, type, context);
    }

    protected XmlWriteHandler lookupWriter(Object value, Class type, OutputNode output, XmlWriteContext context) throws Exception {
        return registry.lookupWriter(value, type, output, context);
    }

    protected boolean isReference(Value value) {
        return value != null && value.isReference();
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