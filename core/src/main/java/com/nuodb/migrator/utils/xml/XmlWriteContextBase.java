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

import org.simpleframework.xml.stream.NodeMap;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class XmlWriteContextBase implements XmlWriteContext {

    private Map map;
    private XmlWriteContext context;

    protected XmlWriteContextBase() {
        this(newHashMap());
    }

    protected XmlWriteContextBase(Map map) {
        this.map = map;
    }

    @Override
    public boolean skip(OutputNode output, Object source) {
        return skip(output, source, source != null ? source.getClass() : null);
    }

    @Override
    public boolean skip(OutputNode output, Object source, XmlWriteContext context) {
        return skip(output, source, source != null ? source.getClass() : null, context);
    }

    @Override
    public boolean skip(OutputNode output, Object source, Class type) {
        XmlWriteContext context = getContext();
        return skip(output, source, type, context != null ? context : this);
    }

    @Override
    public void write(OutputNode output, Object source) {
        if (source != null) {
            write(output, source, source.getClass());
        }
    }

    @Override
    public void write(OutputNode output, Object source, Class type) {
        XmlWriteContext context = getContext();
        write(output, source, type, context != null ? context : this);
    }

    @Override
    public OutputNode writeAttribute(OutputNode output, String attribute, Object source) {
        return writeAttribute(output, null, attribute, source);
    }

    @Override
    public OutputNode writeAttribute(OutputNode output, String namespace, String attribute, Object source) {
        OutputNode node = null;
        if (!skip(null, source)) {
            NodeMap<OutputNode> attributes = output.getAttributes();
            node = attributes.get(attribute);
            if (node == null) {
                node = attributes.put(attribute, EMPTY);
            }
            node.setReference(namespace == null ? EMPTY : namespace);
            write(node, source);
        }
        return node;
    }

    @Override
    public OutputNode writeElement(OutputNode output, String element, Object value) {
        return writeElement(output, null, element, value);
    }

    @Override
    public OutputNode writeElement(OutputNode output, String namespace, String element, Object source) {
        OutputNode node = null;
        if (!skip(null, source)) {
            try {
                node = output.getChild(element);
            } catch (Exception exception) {
                throw new XmlPersisterException(exception);
            }
            node.setReference(namespace == null ? EMPTY : namespace);
            write(node, source);
        }
        return node;
    }

    @Override
    public Map getMap() {
        return map;
    }

    @Override
    public void setMap(Map map) {
        this.map = map;
    }

    @Override
    public XmlWriteContext getContext() {
        return context;
    }

    @Override
    public void setContext(XmlWriteContext context) {
        this.context = context;
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        XmlWriteContextBase that = (XmlWriteContextBase) o;

        if (map != null ? !map.equals(that.map) : that.map != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map != null ? map.hashCode() : 0;
    }
}
