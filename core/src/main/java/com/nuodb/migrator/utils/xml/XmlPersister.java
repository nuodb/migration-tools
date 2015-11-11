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

import org.simpleframework.xml.core.Persister;
import org.simpleframework.xml.strategy.Strategy;
import org.simpleframework.xml.stream.Format;
import org.simpleframework.xml.stream.HyphenStyle;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

public class XmlPersister {

    public static final boolean USE_XML10_FILTER_READER = true;
    public static final int INDENT = 2;
    public static final String PROLOG = "<?xml version=\"1.0\"?>";

    private Strategy strategy;
    private Format format;
    private boolean useXml10FilterReader = USE_XML10_FILTER_READER;

    public XmlPersister(XmlHandlerRegistry registry) {
        this(new XmlHandlerStrategy(registry));
    }

    public XmlPersister(XmlHandlerRegistry registry, Map context) {
        this(new XmlHandlerStrategy(registry, context));
    }

    public XmlPersister(Strategy strategy) {
        this(strategy, null);
    }

    public XmlPersister(Strategy strategy, Format format) {
        this.strategy = strategy;
        this.format = format == null ? new Format(INDENT, PROLOG, new HyphenStyle()) : format;
    }

    public <T> T read(Class<T> type, Reader reader) {
        return read(type, reader, null);
    }

    public <T> T read(Class<T> type, Reader reader, Map context) {
        try {
            return createPersister(context).read(type,
                    isUseXml10FilterReader() ? new Xml10FilterReader(reader) : reader);
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
    }

    public <T> T read(Class<T> type, InputStream input) {
        return read(type, input, null);
    }

    public <T> T read(Class<T> type, InputStream input, Map context) {
        return read(type, new InputStreamReader(input), context);
    }

    public void write(Object source, Writer writer) {
        write(source, writer, null);
    }

    public void write(Object source, Writer writer, Map context) {
        try {
            createPersister(context).write(source, writer);
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
    }

    public void write(Object source, OutputStream output) {
        write(source, output, null);
    }

    public void write(Object source, OutputStream output, Map context) {
        try {
            createPersister(context).write(source, output);
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
    }

    protected Persister createPersister(Map context) {
        if (strategy instanceof XmlContextStrategy) {
            ((XmlContextStrategy) strategy).setContext(context);
        }
        return new Persister(strategy, format);
    }

    public boolean isUseXml10FilterReader() {
        return useXml10FilterReader;
    }

    public void setUseXml10FilterReader(boolean useXml10FilterReader) {
        this.useXml10FilterReader = useXml10FilterReader;
    }
}
