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

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class XmlReadTargetAwareContext<T> implements XmlReadContext {

    private T target;
    private XmlReadContext context;

    public XmlReadTargetAwareContext(T target, XmlReadContext context) {
        this.target = target;
        this.context = context;
    }

    @Override
    public <T> T read(InputNode node, Class<T> type) {
        return read(node, type, this);
    }

    @Override
    public <T> T read(InputNode node, Class<T> type, T defaultValue) {
        return context.read(node, type, defaultValue);
    }

    @Override
    public <T> T read(InputNode input, Class<T> type, XmlReadContext delegate) {
        return context.read(input, type, delegate);
    }

    @Override
    public <T> T readAttribute(InputNode input, String attribute, Class<T> type) {
        return context.readAttribute(input, attribute, type);
    }

    @Override
    public <T> T readAttribute(InputNode input, String attribute, Class<T> type, T defaultValue) {
        return context.readAttribute(input, attribute, type, defaultValue);
    }

    @Override
    public <T> T readElement(InputNode input, String element, Class<T> type) {
        return context.readElement(input, element, type);
    }

    @Override
    public <T> T readElement(InputNode input, String element, Class<T> type, T defaultValue) {
        return context.readElement(input, element, type, defaultValue);
    }

    @Override
    public Map getMap() {
        return context.getMap();
    }

    @Override
    public void setMap(Map map) {
        context.setMap(map);
    }

    public T getTarget() {
        return target;
    }

    public <T> T getTarget(int parent) {
        XmlReadContext context = this;
        for (int index = 0; context != null && index < parent; index++) {
            context = context.getContext();
        }
        return context instanceof XmlReadTargetAwareContext ? (T) ((XmlReadTargetAwareContext) context).getTarget()
                : null;
    }

    public void setTarget(T target) {
        this.target = target;
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
        return context.size();
    }

    @Override
    public boolean isEmpty() {
        return context.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return context.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return context.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return context.get(key);
    }

    @Override
    public Object put(Object key, Object value) {
        return context.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return context.remove(key);
    }

    @Override
    public void putAll(Map m) {
        context.putAll(m);
    }

    @Override
    public void clear() {
        context.clear();
    }

    @Override
    public Set keySet() {
        return context.keySet();
    }

    @Override
    public Collection values() {
        return context.values();
    }

    @Override
    public Set<Entry> entrySet() {
        return context.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        XmlReadTargetAwareContext that = (XmlReadTargetAwareContext) o;

        if (context != null ? !context.equals(that.context) : that.context != null)
            return false;
        if (target != null ? !target.equals(that.target) : that.target != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = target != null ? target.hashCode() : 0;
        result = 31 * result + (context != null ? context.hashCode() : 0);
        return result;
    }
}
