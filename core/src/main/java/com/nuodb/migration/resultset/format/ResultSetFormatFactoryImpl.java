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
package com.nuodb.migration.resultset.format;

import com.google.common.collect.Maps;
import com.nuodb.migration.resultset.format.bson.BsonAttributes;
import com.nuodb.migration.resultset.format.bson.BsonResultSetInput;
import com.nuodb.migration.resultset.format.bson.BsonResultSetOutput;
import com.nuodb.migration.resultset.format.csv.CsvAttributes;
import com.nuodb.migration.resultset.format.csv.CsvResultSetInput;
import com.nuodb.migration.resultset.format.csv.CsvResultSetOutput;
import com.nuodb.migration.resultset.format.xml.XmlAttributes;
import com.nuodb.migration.resultset.format.xml.XmlResultSetInput;
import com.nuodb.migration.resultset.format.xml.XmlResultSetOutput;
import com.nuodb.migration.utils.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class ResultSetFormatFactoryImpl implements ResultSetFormatFactory {

    protected final Log log = LogFactory.getLog(getClass());

    private Map<String, Class<? extends ResultSetInput>> resultSetInputClasses = Maps.newHashMap();

    private Map<String, Class<? extends ResultSetOutput>> resultSetOutputClasses = Maps.newHashMap();

    public ResultSetFormatFactoryImpl() {
        registerResultSetInput(CsvAttributes.FORMAT_TYPE, CsvResultSetInput.class);
        registerResultSetInput(XmlAttributes.FORMAT_TYPE, XmlResultSetInput.class);
        registerResultSetInput(BsonAttributes.FORMAT_TYPE, BsonResultSetInput.class);

        registerResultSetOutput(CsvAttributes.FORMAT_TYPE, CsvResultSetOutput.class);
        registerResultSetOutput(XmlAttributes.FORMAT_TYPE, XmlResultSetOutput.class);
        registerResultSetOutput(BsonAttributes.FORMAT_TYPE, BsonResultSetOutput.class);
    }

    @Override
    public ResultSetInput createResultSetInput(String formatType) {
        return createResultSetFormat(formatType, resultSetInputClasses.get(formatType));
    }

    @Override
    public ResultSetOutput createResultSetOutput(String formatType) {
        return createResultSetFormat(formatType, resultSetOutputClasses.get(formatType));
    }

    @Override
    public void registerResultSetInput(String formatType, Class<? extends ResultSetInput> inputClass) {
        resultSetInputClasses.put(formatType, inputClass);
    }

    @Override
    public void registerResultSetOutput(String formatType, Class<? extends ResultSetOutput> outputClass) {
        resultSetOutputClasses.put(formatType, outputClass);
    }

    protected <T extends ResultSetFormat> T createResultSetFormat(String formatType, Class<T> formatClass) {
        if (formatClass == null) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Can't resolve format type %1$s to a class", formatType));
            }
            ClassLoader classLoader = ClassUtils.getClassLoader();
            try {
                formatClass = (Class<T>) classLoader.loadClass(formatType);
            } catch (ClassNotFoundException e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Loading %1$s as class failed", formatType));
                }
            }
            if (formatClass == null) {
                throw new ResultSetInputException(String.format("Format %1$s is not recognized", formatType));
            }
        }
        return ClassUtils.newInstance(formatClass);
    }
}
