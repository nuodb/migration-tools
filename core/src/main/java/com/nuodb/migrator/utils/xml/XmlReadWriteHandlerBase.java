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
import org.simpleframework.xml.stream.OutputNode;
import org.slf4j.Logger;

import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;
import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("unchecked")
public abstract class XmlReadWriteHandlerBase<T> extends XmlAttributesAccessor implements XmlReadWriteHandler<T> {

    protected final transient Logger logger = getLogger(getClass());

    private Class type;

    protected XmlReadWriteHandlerBase(Class type) {
        this.type = type;
    }

    @Override
    public T read(InputNode input, Class<? extends T> type, XmlReadContext parent) {
        XmlReadTargetAwareContext<T> context = createContext(input, type, parent);
        try {
            read(input, context);
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
        return context.getTarget();
    }

    protected XmlReadTargetAwareContext<T> createContext(InputNode input, Class<? extends T> type,
            XmlReadContext context) {
        return new XmlReadTargetAwareContext<T>(createTarget(input, type, context), context);
    }

    protected T createTarget(InputNode input, Class<? extends T> type, XmlReadContext context) {
        return createTarget(input, type);
    }

    protected T createTarget(InputNode input, Class<? extends T> type) {
        return newInstance(type);
    }

    protected <T> T getParent(XmlReadContext context) {
        return getParent(context, 1);
    }

    protected <T> T getParent(XmlReadContext context, int parent) {
        return context instanceof XmlReadTargetAwareContext
                ? (T) ((XmlReadTargetAwareContext) context).getTarget(parent)
                : null;
    }

    protected void read(InputNode input, XmlReadTargetAwareContext<T> context) throws Exception {
        readAttributes(input, context);
        readElements(input, context);
    }

    protected void readAttributes(InputNode input, XmlReadTargetAwareContext<T> context) throws Exception {
        readAttributes(input, context.getTarget(), context);
    }

    protected void readAttributes(InputNode input, T target, XmlReadContext context) throws Exception {
    }

    protected void readElements(InputNode input, XmlReadTargetAwareContext<T> context) throws Exception {
        readElements(input, context.getTarget(), context);
    }

    protected void readElements(InputNode input, T target, XmlReadContext context) throws Exception {
        InputNode node;
        while ((node = input.getNext()) != null) {
            readElement(node, target, context);
        }
    }

    protected void readElement(InputNode input, XmlReadTargetAwareContext<T> context) throws Exception {
        readElement(input, context.getTarget(), context);
    }

    protected void readElement(InputNode input, T target, XmlReadContext context) throws Exception {
    }

    @Override
    public boolean canRead(InputNode input, Class type, XmlReadContext context) {
        return getType().equals(type);
    }

    @Override
    public boolean skip(T source, Class<? extends T> type, OutputNode output, XmlWriteContext context) {
        return skip(source, context);
    }

    protected boolean skip(T source, XmlWriteContext context) {
        return false;
    }

    @Override
    public boolean write(T source, Class<? extends T> type, OutputNode output, XmlWriteContext parent) {
        try {
            return write(output, createContext(source, output, parent));
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
    }

    protected XmlWriteSourceAwareContext<T> createContext(T source, OutputNode output, XmlWriteContext context) {
        return new XmlWriteSourceAwareContext(source, context);
    }

    protected <T> T getParent(XmlWriteContext context) {
        return getParent(context, 1);
    }

    protected <T> T getParent(XmlWriteContext context, int parent) {
        return context instanceof XmlWriteSourceAwareContext
                ? (T) ((XmlWriteSourceAwareContext) context).getSource(parent)
                : null;
    }

    protected boolean write(OutputNode output, XmlWriteSourceAwareContext context) throws Exception {
        writeAttributes(output, context);
        writeElements(output, context);
        return true;
    }

    protected void writeAttributes(OutputNode output, XmlWriteSourceAwareContext<T> context) throws Exception {
        writeAttributes(context.getSource(), output, context);
    }

    protected void writeAttributes(T source, OutputNode output, XmlWriteContext context) throws Exception {
    }

    protected void writeElements(OutputNode output, XmlWriteSourceAwareContext<T> context) throws Exception {
        writeElements(context.getSource(), output, context);
    }

    protected void writeElements(T source, OutputNode output, XmlWriteContext context) throws Exception {
    }

    @Override
    public boolean canWrite(Object source, Class type, OutputNode output, XmlWriteContext context) {
        return getType().equals(type);
    }

    public Class getType() {
        return type;
    }
}
