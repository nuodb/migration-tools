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

import org.simpleframework.xml.stream.OutputNode;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class XmlWriteSourceAwareContext<T> implements XmlWriteContext {

    private T source;
    private XmlWriteContext context;

    public XmlWriteSourceAwareContext(T source, XmlWriteContext context) {
        this.source = source;
        this.context = context;
    }

    @Override
    public boolean skip(OutputNode output, Object source) {
        return context.skip(output, source, this);
    }

    @Override
    public boolean skip(OutputNode output, Object source, XmlWriteContext delegate) {
        return context.skip(output, source, delegate);
    }

    @Override
    public boolean skip(OutputNode output, Object source, Class type) {
        return context.skip(output, source, type, this);
    }

    @Override
    public boolean skip(OutputNode output, Object source, Class type, XmlWriteContext delegate) {
        return context.skip(output, source, type, delegate);
    }

    @Override
    public void write(OutputNode output, Object source) {
        context.write(output, source);
    }

    @Override
    public void write(OutputNode output, Object source, Class type) {
        context.write(output, source, type, this);
    }

    @Override
    public void write(OutputNode output, Object source, Class type, XmlWriteContext delegate) {
        context.write(output, source, type, delegate);
    }

    @Override
    public OutputNode writeAttribute(OutputNode output, String attribute, Object source) {
        return context.writeAttribute(output, attribute, source);
    }

    @Override
    public OutputNode writeAttribute(OutputNode output, String namespace, String attribute, Object source) {
        return context.writeAttribute(output, namespace, attribute, source);
    }

    @Override
    public OutputNode writeElement(OutputNode output, String attribute, Object element) {
        return context.writeElement(output, attribute, element);
    }

    @Override
    public OutputNode writeElement(OutputNode output, String namespace, String element, Object source) {
        return context.writeElement(output, namespace, element, source);
    }

    public T getSource() {
        return source;
    }

    public <T> T getSource(int parent) {
        XmlWriteContext context = this;
        for (int index = 0; context != null && index < parent; index++) {
            context = context.getContext();
        }
        return context instanceof XmlWriteSourceAwareContext ? (T) ((XmlWriteSourceAwareContext) context).getSource()
                : null;
    }

    public void setSource(T source) {
        this.source = source;
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
    public Map getMap() {
        return context.getMap();
    }

    @Override
    public void setMap(Map map) {
        context.setMap(map);
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

        XmlWriteSourceAwareContext that = (XmlWriteSourceAwareContext) o;

        if (context != null ? !context.equals(that.context) : that.context != null)
            return false;
        if (source != null ? !source.equals(that.source) : that.source != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = source != null ? source.hashCode() : 0;
        result = 31 * result + (context != null ? context.hashCode() : 0);
        return result;
    }
}
