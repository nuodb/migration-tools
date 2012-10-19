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
package com.nuodb.tools.migration.dump.output;

import com.nuodb.tools.migration.utils.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class OutputFormatLookupImpl implements OutputFormatLookup {

    protected final Log log = LogFactory.getLog(getClass());

    private Class<? extends OutputFormat> defaultFormatClass = CsvOutputFormat.class;
    private Map<String, Class<? extends OutputFormat>> formatsClasses = new HashMap<String, Class<? extends OutputFormat>>();

    public OutputFormatLookupImpl() {
        register(XmlOutputFormat.EXTENSION, XmlOutputFormat.class);
        register(CsvOutputFormat.TYPE, CsvOutputFormat.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public OutputFormat lookup(String type) {
        Class<? extends OutputFormat> formatClass = formatsClasses.get(type);
        if (formatClass == null) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Can't resolve output format type %1$s to a class", type));
            }
            ClassLoader classLoader = ClassUtils.getClassLoader();
            try {
                formatClass = (Class<? extends OutputFormat>) classLoader.loadClass(type);
            } catch (ClassNotFoundException e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Loading %1$s as class failed", type));
                }
            }
            if (formatClass == null) {
                formatClass = getDefaultFormatClass();
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Defaulting output format to %1$s", formatClass.getName()));
                }
            }
        }
        return newInstance(formatClass);
    }

    @Override
    public void register(String type, Class<? extends OutputFormat> formatClass) {
        formatsClasses.put(type, formatClass);
    }

    protected OutputFormat newInstance(Class<? extends OutputFormat> formatClass) {
        return ClassUtils.newInstance(formatClass);
    }

    public Class<? extends OutputFormat> getDefaultFormatClass() {
        return defaultFormatClass;
    }

    public void setDefaultFormatClass(Class<? extends OutputFormat> defaultFormatClass) {
        this.defaultFormatClass = defaultFormatClass;
    }
}
