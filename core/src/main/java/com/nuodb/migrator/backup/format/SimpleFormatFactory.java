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
package com.nuodb.migrator.backup.format;

import com.nuodb.migrator.backup.format.bson.BsonFormat;
import com.nuodb.migrator.backup.format.bson.BsonInput;
import com.nuodb.migrator.backup.format.bson.BsonOutput;
import com.nuodb.migrator.backup.format.csv.CsvFormat;
import com.nuodb.migrator.backup.format.csv.CsvInput;
import com.nuodb.migrator.backup.format.csv.CsvOutput;
import com.nuodb.migrator.backup.format.xml.XmlFormat;
import com.nuodb.migrator.backup.format.xml.XmlInput;
import com.nuodb.migrator.backup.format.xml.XmlOutput;
import org.slf4j.Logger;

import java.util.Map;
import java.util.TreeMap;

import static com.nuodb.migrator.utils.ReflectionUtils.getClassLoader;
import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleFormatFactory implements FormatFactory {

    protected final Logger logger = getLogger(getClass());

    private Map<String, Class<? extends Input>> inputFormats = new TreeMap<String, Class<? extends Input>>(
            CASE_INSENSITIVE_ORDER);

    private Map<String, Class<? extends Output>> outputFormats = new TreeMap<String, Class<? extends Output>>(
            CASE_INSENSITIVE_ORDER);

    public SimpleFormatFactory() {
        addFormat(CsvFormat.TYPE, CsvInput.class);
        addFormat(XmlFormat.TYPE, XmlInput.class);
        addFormat(BsonFormat.TYPE, BsonInput.class);

        addFormat(CsvFormat.TYPE, CsvOutput.class);
        addFormat(XmlFormat.TYPE, XmlOutput.class);
        addFormat(BsonFormat.TYPE, BsonOutput.class);
    }

    public void addFormat(String format, Class<? extends Format> formatClass) {
        if (Output.class.isAssignableFrom(formatClass)) {
            outputFormats.put(format, (Class<? extends Output>) formatClass);
        }
        if (Input.class.isAssignableFrom(formatClass)) {
            inputFormats.put(format, (Class<? extends Input>) formatClass);
        }
    }

    @Override
    public Input createInput(String format, Map<String, Object> attributes) {
        return (Input) createFormat(format, inputFormats.get(format), attributes);
    }

    @Override
    public Output createOutput(String format, Map<String, Object> attributes) {
        return (Output) createFormat(format, outputFormats.get(format), attributes);
    }

    protected Format createFormat(String type, Class<? extends Format> formatClass, Map<String, Object> attributes) {
        if (formatClass == null) {
            if (logger.isTraceEnabled()) {
                logger.trace(format("Can't resolve format %s to a class", type));
            }
            ClassLoader classLoader = getClassLoader();
            try {
                formatClass = (Class<? extends Format>) classLoader.loadClass(type);
            } catch (ClassNotFoundException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn(format("Loading %s as class failed", type));
                }
            }
            if (formatClass == null) {
                throw new InputException(format("Format %s is not supported", type));
            }
        }
        Format format = newInstance(formatClass);
        format.setAttributes(attributes);
        return format;
    }
}
