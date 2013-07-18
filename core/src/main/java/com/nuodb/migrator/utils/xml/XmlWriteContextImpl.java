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
package com.nuodb.migrator.utils.xml;

import org.simpleframework.xml.strategy.Strategy;
import org.simpleframework.xml.stream.NodeMap;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@SuppressWarnings("unchecked")
public class XmlWriteContextImpl implements XmlWriteContext {

    private Strategy strategy;
    private Map map;

    public XmlWriteContextImpl(Strategy strategy, Map map) {
        this.strategy = strategy;
        this.map = map;
    }

    @Override
    public void write(OutputNode output, Object value) {
        if (value != null) {
            write(output, value, value.getClass());
        }
    }

    @Override
    public void write(OutputNode output, Object value, Class type) {
        boolean complete;
        try {
            complete = strategy.write(new ClassType(type), value, output.getAttributes(), this);
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
        if (!complete) {
            throw new XmlPersisterException(format("Appropriate handler required to serialize %s to %s", type, output));
        }
    }

    @Override
    public OutputNode writeAttribute(OutputNode output, String attribute, Object value) {
        return writeAttribute(output, null, attribute, value);
    }

    @Override
    public OutputNode writeAttribute(OutputNode output, String namespace, String attribute, Object value) {
        NodeMap<OutputNode> attributes = output.getAttributes();
        OutputNode node = attributes.get(attribute);
        if (node == null) {
            node = attributes.put(attribute, EMPTY);
        }
        node.setReference(namespace == null ? EMPTY : namespace);
        write(node, value);
        return node;
    }

    @Override
    public OutputNode writeElement(OutputNode output, String element, Object value) {
        return writeElement(output, null, element, value);
    }

    @Override
    public OutputNode writeElement(OutputNode output, String namespace, String element, Object value) {
        OutputNode node;
        try {
            node = output.getChild(element);
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
        node.setReference(namespace == null ? EMPTY : namespace);
        write(node, value);
        return node;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return map.get(key);
    }

    @Override
    public Object put(Object key, Object value) {
        return map.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(Map m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set keySet() {
        return map.keySet();
    }

    @Override
    public Collection values() {
        return map.values();
    }

    @Override
    public Set entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        XmlWriteContextImpl that = (XmlWriteContextImpl) o;

        if (map != null ? !map.equals(that.map) : that.map != null) return false;
        if (strategy != null ? !strategy.equals(that.strategy) : that.strategy != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = strategy != null ? strategy.hashCode() : 0;
        result = 31 * result + (map != null ? map.hashCode() : 0);
        return result;
    }
}
