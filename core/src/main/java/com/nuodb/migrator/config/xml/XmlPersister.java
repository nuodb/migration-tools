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
package com.nuodb.migrator.config.xml;

import org.simpleframework.xml.core.Persister;
import org.simpleframework.xml.stream.Format;

import java.io.InputStream;
import java.io.OutputStream;

public class XmlPersister {

    private static final String PROLOG = "<?xml version=\"1.0\"?>";

    private Persister persister;

    public XmlPersister(XmlHandlerRegistry registry) {
        this(new XmlHandlerStrategy(registry));
    }

    public XmlPersister(XmlHandlerStrategy strategy) {
        this(strategy, new Format(PROLOG));
    }

    public XmlPersister(XmlHandlerStrategy strategy, Format format) {
        this.persister = new Persister(strategy, format);
    }

    public <T> T read(Class<T> type, InputStream input) {
        try {
            return persister.read(type, input);
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }

    public void write(Object source, OutputStream output) {
        try {
            persister.write(source, output);
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }
}
