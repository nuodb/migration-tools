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
package com.nuodb.migrator.resultset.format;

import com.nuodb.migrator.resultset.format.bson.BsonAttributes;
import com.nuodb.migrator.resultset.format.bson.BsonInputFormat;
import com.nuodb.migrator.resultset.format.bson.BsonOutputFormat;
import com.nuodb.migrator.resultset.format.csv.CsvAttributes;
import com.nuodb.migrator.resultset.format.csv.CsvInputFormat;
import com.nuodb.migrator.resultset.format.csv.CsvOutputFormat;
import com.nuodb.migrator.resultset.format.xml.XmlAttributes;
import com.nuodb.migrator.resultset.format.xml.XmlInputFormat;
import com.nuodb.migrator.resultset.format.xml.XmlOutputFormat;
import com.nuodb.migrator.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

import static java.lang.String.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleFormatFactory implements FormatFactory {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Class<? extends FormatInput>> inputClasses =
            new TreeMap<String, Class<? extends FormatInput>>(CASE_INSENSITIVE_ORDER);

    private Map<String, Class<? extends FormatOutput>> outputClasses =
            new TreeMap<String, Class<? extends FormatOutput>>(CASE_INSENSITIVE_ORDER);

    public SimpleFormatFactory() {
        registerFormat(CsvAttributes.FORMAT_TYPE, CsvInputFormat.class);
        registerFormat(XmlAttributes.FORMAT_TYPE, XmlInputFormat.class);
        registerFormat(BsonAttributes.FORMAT_TYPE, BsonInputFormat.class);

        registerFormat(CsvAttributes.FORMAT_TYPE, CsvOutputFormat.class);
        registerFormat(XmlAttributes.FORMAT_TYPE, XmlOutputFormat.class);
        registerFormat(BsonAttributes.FORMAT_TYPE, BsonOutputFormat.class);
    }

    @Override
    public FormatInput createInput(String formatType) {
        return (FormatInput) createFormat(formatType, inputClasses.get(formatType));
    }

    @Override
    public FormatOutput createOutput(String formatType) {
        return (FormatOutput) createFormat(formatType, outputClasses.get(formatType));
    }

    @Override
    public void registerFormat(String formatType, Class<? extends Format> formatClass) {
        if (FormatOutput.class.isAssignableFrom(formatClass)) {
            outputClasses.put(formatType, (Class<? extends FormatOutput>) formatClass);
        }
        if (FormatInput.class.isAssignableFrom(formatClass)) {
            inputClasses.put(formatType, (Class<? extends FormatInput>) formatClass);
        }
    }

    protected Format createFormat(String formatType, Class<? extends Format> formatClass) {
        if (formatClass == null) {
            if (logger.isTraceEnabled()) {
                logger.trace(format("Can't resolve format type %1$s to a class", formatType));
            }
            ClassLoader classLoader = ReflectionUtils.getClassLoader();
            try {
                formatClass = (Class<? extends Format>) classLoader.loadClass(formatType);
            } catch (ClassNotFoundException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn(format("Loading %1$s as class failed", formatType));
                }
            }
            if (formatClass == null) {
                throw new FormatInputException(format("Format %1$s is not recognized", formatType));
            }
        }
        return ReflectionUtils.newInstance(formatClass);
    }
}
