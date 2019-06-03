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

import com.nuodb.migrator.jdbc.connection.ConnectionProxy;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.type.JdbcValueAccess;

import java.sql.Connection;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public abstract class ValueFormatBase<T> implements ValueFormat<T> {

    @Override
    public Value getValue(JdbcValueAccess<T> access, Map<String, Object> options) {
        try {
            return doGetValue(access, options);
        } catch (ValueFormatException exception) {
            throw exception;
        } catch (Throwable cause) {
            return onGetValueError(access, cause);
        }
    }

    protected abstract Value doGetValue(JdbcValueAccess<T> access, Map<String, Object> options) throws Throwable;

    protected Value onGetValueError(JdbcValueAccess access, Throwable cause) {
        throw new ValueFormatException(format("Can't get %s %s column value", getColumnName(access.getField()),
                access.getField().getTypeName()), cause);
    }

    @Override
    public void setValue(Value value, JdbcValueAccess<T> access, Map<String, Object> options) {
        try {
            doSetValue(value, access, options);
        } catch (ValueFormatException exception) {
            throw exception;
        } catch (Throwable cause) {
            onSetValueError(access, cause);
        }
    }

    protected abstract void doSetValue(Value value, JdbcValueAccess<T> access, Map<String, Object> options)
            throws Throwable;

    protected void onSetValueError(JdbcValueAccess access, Throwable cause) {
        throw new ValueFormatException(format("Can't set %s %s column value", getColumnName(access.getField()),
                access.getField().getTypeName()), cause);
    }

    protected String getColumnName(Field field) {
        if (field instanceof Column) {
            Column column = (Column) field;
            Table table = column.getTable();
            Dialect dialect = table.getDatabase().getDialect();
            return format("table %s %s", table.getQualifiedName(dialect), column.getName(dialect));
        } else {
            return field.getName();
        }
    }

    protected Connection getConnection(JdbcValueAccess<Object> access) {
        Connection connection = access.getConnection();
        if (connection instanceof ConnectionProxy) {
            connection = ((ConnectionProxy) connection).getConnection();
        }
        return connection;
    }
}
