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

import com.nuodb.migrator.utils.Priority;
import com.nuodb.migrator.utils.PrioritySet;
import org.simpleframework.xml.convert.Converter;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.utils.Collections.newPrioritySet;

@SuppressWarnings("unchecked")
public class XmlHandlerRegistry {

    private PrioritySet<XmlReadHandler> readers = newPrioritySet();
    private PrioritySet<XmlWriteHandler> writers = newPrioritySet();

    public XmlHandlerRegistry registerHandler(XmlHandler handler) {
        return registerHandler(handler, Priority.NORMAL);
    }

    public XmlHandlerRegistry registerHandler(XmlHandler handler, int priority) {
        assert (handler instanceof XmlReadHandler || handler instanceof XmlWriteHandler);
        if (handler instanceof XmlWriteHandler) {
            writers.add((XmlWriteHandler) handler, priority);
        }
        if (handler instanceof XmlReadHandler) {
            readers.add((XmlReadHandler) handler, priority);
        }
        return this;
    }

    public XmlWriteHandler lookupWriter(Object value, Class type, OutputNode output, XmlWriteContext context) {
        for (XmlWriteHandler writer : writers) {
            if (writer.canWrite(value, type, output, context)) {
                return writer;
            }
        }
        return null;
    }

    public XmlReadHandler lookupReader(InputNode input, Class type, XmlReadContext context) {
        for (XmlReadHandler reader : readers) {
            if (reader.canRead(input, type, context)) {
                return reader;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    class ConverterAdapter implements XmlReadWriteHandler {

        private Class type;
        private Converter converter;

        public ConverterAdapter(Class type, Converter converter) {
            this.type = type;
            this.converter = converter;
        }

        @Override
        public Object read(InputNode input, Class type, XmlReadContext context) {
            try {
                return converter.read(input);
            } catch (Exception e) {
                throw new XmlPersisterException("Underlying converter failed to read", e);
            }
        }

        @Override
        public boolean canRead(InputNode input, Class type, XmlReadContext context) {
            return this.type.equals(type);
        }

        @Override
        public boolean skip(Object source, Class type, OutputNode output, XmlWriteContext context) {
            return false;
        }

        @Override
        public boolean write(Object source, Class type, OutputNode output, XmlWriteContext context) {
            try {
                converter.write(output, source);
                return true;
            } catch (Exception e) {
                throw new XmlPersisterException("Underlying converter failed to export", e);
            }
        }

        @Override
        public boolean canWrite(Object source, Class type, OutputNode output, XmlWriteContext context) {
            return this.type.equals(type);
        }
    }

}
