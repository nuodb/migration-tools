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
package com.nuodb.migration.resultset.format.jdbc;

import com.nuodb.migration.jdbc.metadata.ColumnModel;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccess;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
public abstract class JdbcTypeValueFormatBase<T> implements JdbcTypeValueFormat<T> {

    @Override
    public String getValue(JdbcTypeValueAccess<T> access, Map<String, Object> options) {
        try {
            return doGetValue(access, options);
        } catch (JdbcTypeValueException exception) {
            throw exception;
        } catch (Exception exception) {
            throw newGetValueFailure(access, exception);
        }
    }

    protected abstract String doGetValue(JdbcTypeValueAccess<T> access, Map<String, Object> options) throws Exception;

    protected JdbcTypeValueException newGetValueFailure(JdbcTypeValueAccess access, Exception exception) {
        ColumnModel column = access.getColumnModel();
        return new JdbcTypeValueException(
                String.format("Can't get column %s type %s value",
                        column.getName(), column.getTypeName()), exception);
    }

    @Override
    public void setValue(JdbcTypeValueAccess<T> access, String value, Map<String, Object> options) {
        try {
            doSetValue(access, value, options);
        } catch (JdbcTypeValueException exception) {
            throw exception;
        } catch (Exception exception) {
            throw newSetValueFailure(access, exception);
        }
    }

    protected abstract void doSetValue(JdbcTypeValueAccess<T> access, String value,
                                       Map<String, Object> options) throws Exception;

    protected JdbcTypeValueException newSetValueFailure(JdbcTypeValueAccess access, Exception exception) {
        ColumnModel column = access.getColumnModel();
        return new JdbcTypeValueException(
                String.format("Can't set column %s type %s value",
                        column.getName(), column.getTypeName()), exception);
    }
}


