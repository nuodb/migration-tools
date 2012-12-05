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
import com.nuodb.migration.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DefaultResultSetFormatFactory implements ResultSetFormatFactory {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Class<? extends ResultSetInput>> inputClasses = Maps.newHashMap();

    private Map<String, Class<? extends ResultSetOutput>> outputClasses = Maps.newHashMap();

    public DefaultResultSetFormatFactory() {
        registerFormat(CsvAttributes.FORMAT_TYPE, CsvResultSetInput.class);
        registerFormat(XmlAttributes.FORMAT_TYPE, XmlResultSetInput.class);
        registerFormat(BsonAttributes.FORMAT_TYPE, BsonResultSetInput.class);

        registerFormat(CsvAttributes.FORMAT_TYPE, CsvResultSetOutput.class);
        registerFormat(XmlAttributes.FORMAT_TYPE, XmlResultSetOutput.class);
        registerFormat(BsonAttributes.FORMAT_TYPE, BsonResultSetOutput.class);
    }

    @Override
    public ResultSetInput createInput(String formatType) {
        return createFormat(formatType, inputClasses.get(formatType));
    }

    @Override
    public ResultSetOutput createOutput(String formatType) {
        return createFormat(formatType, outputClasses.get(formatType));
    }

    @Override
    public void registerFormat(String formatType, Class<? extends ResultSetFormat> formatClass) {
        if (ResultSetOutput.class.isAssignableFrom(formatClass)) {
            outputClasses.put(formatType, (Class<? extends ResultSetOutput>) formatClass);
        }
        if (ResultSetInput.class.isAssignableFrom(formatClass)) {
            inputClasses.put(formatType, (Class<? extends ResultSetInput>) formatClass);
        }
    }

    protected <T extends ResultSetFormat> T createFormat(String formatType, Class<T> formatClass) {
        if (formatClass == null) {
            if (logger.isTraceEnabled()) {
                logger.trace(String.format("Can't resolve format type %1$s to a class", formatType));
            }
            ClassLoader classLoader = ReflectionUtils.getClassLoader();
            try {
                formatClass = (Class<T>) classLoader.loadClass(formatType);
            } catch (ClassNotFoundException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn(String.format("Loading %1$s as class failed", formatType));
                }
            }
            if (formatClass == null) {
                throw new ResultSetInputException(String.format("Format %1$s is not recognized", formatType));
            }
        }
        return ReflectionUtils.newInstance(formatClass);
    }
}
