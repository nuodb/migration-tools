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

import org.simpleframework.xml.stream.InputNode;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class XmlReadContextBase implements XmlReadContext {

    private Map map;
    private XmlReadContext context;

    protected XmlReadContextBase() {
        this(newHashMap());
    }

    protected XmlReadContextBase(Map map) {
        this.map = map;
    }

    @Override
    public <T> T read(InputNode input, Class<T> type) {
        final XmlReadContext context = getContext();
        return read(input, type, context != null ? context : this);
    }

    @Override
    public <T> T read(InputNode node, Class<T> type, T defaultValue) {
        T value = read(node, type);
        return value != null ? value : defaultValue;
    }

    @Override
    public <T> T readAttribute(InputNode input, String attribute, Class<T> type) {
        InputNode node = input.getAttribute(attribute);
        return node != null ? read(node, type) : null;
    }

    @Override
    public <T> T readAttribute(InputNode input, String attribute, Class<T> type, T defaultValue) {
        T value = readAttribute(input, attribute, type);
        return value != null ? value : defaultValue;
    }

    @Override
    public <T> T readElement(InputNode input, String element, Class<T> type) {
        InputNode node;
        try {
            node = input.getNext(element);
        } catch (Exception exception) {
            throw new XmlPersisterException(format("Failed reading %s element from %s", element, input), exception);
        }
        return node != null ? read(node, type) : null;
    }

    @Override
    public <T> T readElement(InputNode input, String element, Class<T> type, T defaultValue) {
        T value = readElement(input, element, type);
        return value != null ? value : defaultValue;
    }

    public abstract <T> T read(InputNode input, Class<T> type, XmlReadContext delegate);

    @Override
    public Map getMap() {
        return map;
    }

    @Override
    public void setMap(Map map) {
        this.map = map;
    }

    @Override
    public XmlReadContext getContext() {
        return context;
    }

    @Override
    public void setContext(XmlReadContext context) {
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

        XmlReadContextBase that = (XmlReadContextBase) o;

        if (map != null ? !map.equals(that.map) : that.map != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map != null ? map.hashCode() : 0;
    }
}
