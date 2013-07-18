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

import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;

@SuppressWarnings("unchecked")
public abstract class XmlReadWriteHandlerBase<T> extends XmlAttributesAccessor implements XmlReadWriteHandler<T> {

    private Class type;

    protected XmlReadWriteHandlerBase(Class type) {
        this.type = type;
    }

    @Override
    public T read(InputNode input, Class<? extends T> type, XmlReadContext context) {
        T target = newInstance(type);
        try {
            read(input, target, context);
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
        return target;
    }

    protected void read(InputNode input, T value, XmlReadContext context) throws Exception {
        readAttributes(input, value, context);
        readElements(input, value, context);
    }

    protected void readAttributes(InputNode input, T value, XmlReadContext context) throws Exception {
    }

    protected void readElements(InputNode input, T value, XmlReadContext context) throws Exception {
        InputNode node;
        while ((node = input.getNext()) != null) {
            readElement(node, value, context);
        }
    }

    protected void readElement(InputNode input, T value, XmlReadContext context) throws Exception {
    }

    @Override
    public boolean canRead(InputNode input, Class type, XmlReadContext context) {
        return this.type.equals(type);
    }

    @Override
    public boolean write(T value, Class<? extends T> type, OutputNode output, XmlWriteContext context) {
        try {
            return write(value, output, context);
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
    }

    protected boolean write(T value, OutputNode output, XmlWriteContext context) throws Exception {
        writeAttributes(value, output, context);
        writeElements(value, output, context);
        return true;
    }

    protected void writeAttributes(T value, OutputNode output, XmlWriteContext context) throws Exception {
    }

    protected void writeElements(T value, OutputNode output, XmlWriteContext context) throws Exception {
    }

    @Override
    public boolean canWrite(Object value, Class type, OutputNode output, XmlWriteContext context) {
        return this.type.equals(type);
    }
}
