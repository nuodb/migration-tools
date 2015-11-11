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
package com.nuodb.migrator.backup.format.value;

import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.type.JdbcValueAccess;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Map;

import static com.nuodb.migrator.backup.format.value.ValueType.STRING;
import static com.nuodb.migrator.backup.format.value.ValueUtils.string;
import static com.nuodb.migrator.utils.ReflectionUtils.invokeMethodNoWrap;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_CLASS_ARRAY;

/**
 * @author Sergey Bushik
 */
public class OracleXmlTypeValueFormat extends LazyInitValueFormatBase<Object> {

    private static final String XML_TYPE_CLASS_NAME = "oracle.xdb.XMLType";
    private static final String OPAQUE_CLASS_NAME = "oracle.sql.OPAQUE";

    private Method stringValue;
    private Method createXML;

    @Override
    protected Value doGetValue(JdbcValueAccess<Object> access, Map<String, Object> options) throws Throwable {
        Object value = access.getValue(options);
        return string(value != null ? (String) invokeMethodNoWrap(value, stringValue) : null);
    }

    @Override
    protected void doSetValue(Value value, JdbcValueAccess<Object> access, Map<String, Object> options)
            throws Throwable {
        access.setValue(invokeMethodNoWrap(null, createXML, access.getConnection(), value.asString()), options);
    }

    @Override
    protected void doLazyInit() {
        ClassLoader classLoader = getClass().getClassLoader();
        try {
            stringValue = classLoader.loadClass(OPAQUE_CLASS_NAME).getMethod("stringValue", EMPTY_CLASS_ARRAY);
            createXML = classLoader.loadClass(XML_TYPE_CLASS_NAME).getMethod("createXML", Connection.class,
                    String.class);
        } catch (Exception exception) {
            throw new ValueFormatException(exception);
        }
    }

    @Override
    public ValueType getValueType(Field field) {
        return STRING;
    }
}
