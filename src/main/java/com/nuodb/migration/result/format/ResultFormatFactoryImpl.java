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
package com.nuodb.migration.result.format;

import com.google.common.collect.Maps;
import com.nuodb.migration.result.format.bson.BsonAttributes;
import com.nuodb.migration.result.format.bson.BsonResultInput;
import com.nuodb.migration.result.format.bson.BsonResultOutput;
import com.nuodb.migration.result.format.csv.CsvAttributes;
import com.nuodb.migration.result.format.csv.CsvResultInput;
import com.nuodb.migration.result.format.csv.CsvResultOutput;
import com.nuodb.migration.result.format.xml.XmlAttributes;
import com.nuodb.migration.result.format.xml.XmlResultInput;
import com.nuodb.migration.result.format.xml.XmlResultOutput;
import com.nuodb.migration.utils.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class ResultFormatFactoryImpl implements ResultFormatFactory {

    protected final Log log = LogFactory.getLog(getClass());

    private Map<String, Class<? extends ResultInput>> inputClasses = Maps.newHashMap();

    private Map<String, Class<? extends ResultOutput>> outputClasses = Maps.newHashMap();

    public ResultFormatFactoryImpl() {
        registerInput(CsvAttributes.TYPE, CsvResultInput.class);
        registerInput(XmlAttributes.TYPE, XmlResultInput.class);
        registerInput(BsonAttributes.TYPE, BsonResultInput.class);

        registerOutput(CsvAttributes.TYPE, CsvResultOutput.class);
        registerOutput(XmlAttributes.TYPE, XmlResultOutput.class);
        registerOutput(BsonAttributes.TYPE, BsonResultOutput.class);
    }

    @Override
    public ResultInput createInput(String formatType) {
        return createFormat(formatType, inputClasses.get(formatType));
    }

    @Override
    public ResultOutput createOutput(String formatType) {
        return createFormat(formatType, outputClasses.get(formatType));
    }

    @Override
    public void registerInput(String formatType, Class<? extends ResultInput> inputClass) {
        inputClasses.put(formatType, inputClass);
    }

    @Override
    public void registerOutput(String formatType, Class<? extends ResultOutput> outputClass) {
        outputClasses.put(formatType, outputClass);
    }

    protected <T extends ResultFormat> T createFormat(String formatType, Class<T> formatClass) {
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
                throw new ResultInputException(String.format("Format %1$s is not recognized", formatType));
            }
        }
        return ClassUtils.newInstance(formatClass);
    }
}
