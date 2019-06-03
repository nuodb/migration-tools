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

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Map;

import static com.nuodb.migrator.backup.format.value.ValueType.BINARY;
import static com.nuodb.migrator.backup.format.value.ValueUtils.binary;
import static com.nuodb.migrator.utils.ReflectionUtils.*;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.io.IOUtils.toByteArray;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_CLASS_ARRAY;

/**
 * @author Sergey Bushik
 */
public class OracleBFileTypeValueFormat extends LazyInitValueFormatBase<Object> {

    private static final String BFILE_CLASS_NAME = "oracle.sql.BFILE";
    private Class<?> bfileClass;
    private Method openFile;
    private Method getBinaryStream;
    private Method closeFile;

    @Override
    protected void doLazyInit() {
        ClassLoader classLoader = getClassLoader();
        try {
            bfileClass = classLoader.loadClass(BFILE_CLASS_NAME);
            openFile = bfileClass.getMethod("openFile", EMPTY_CLASS_ARRAY);
            getBinaryStream = bfileClass.getMethod("getBinaryStream", EMPTY_CLASS_ARRAY);
            closeFile = bfileClass.getMethod("closeFile", EMPTY_CLASS_ARRAY);
        } catch (Exception exception) {
            throw new ValueFormatException(exception);
        }
    }

    @Override
    protected Value doGetValue(JdbcValueAccess<Object> access, Map<String, Object> options) throws Throwable {
        Object value = access.getValue(options);
        byte[] bytes = null;
        if (value != null) {
            invokeMethodNoWrap(value, openFile);
            InputStream input = null;
            try {
                input = invokeMethodNoWrap(value, getBinaryStream);
                bytes = toByteArray(input);
            } finally {
                closeQuietly(input);
                invokeMethodNoWrap(value, closeFile);
            }
        }
        return binary(bytes);
    }

    @Override
    protected void doSetValue(Value value, JdbcValueAccess<Object> access, Map<String, Object> options)
            throws Throwable {
        access.setValue(newInstanceNoWrap(bfileClass, new Object[] { getConnection(access), value.asBytes() }),
                options);
    }

    @Override
    public ValueType getValueType(Field field) {
        return BINARY;
    }
}
