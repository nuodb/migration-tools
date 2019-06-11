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
package com.nuodb.migrator.jdbc.type;

import com.nuodb.migrator.jdbc.model.Field;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class SimpleJdbcValueAccess<T> implements JdbcValueAccess<T> {

    private JdbcValueGetter<T> jdbcValueGetter;
    private JdbcValueSetter jdbcValueSetter;
    private ResultSet resultSet;
    private PreparedStatement statement;
    private Connection connection;
    private int index;
    private Field field;

    public SimpleJdbcValueAccess(JdbcValueGetter<T> jdbcValueGetter, Connection connection, ResultSet resultSet,
            int index, Field field) throws SQLException {
        this.jdbcValueGetter = jdbcValueGetter;
        this.connection = connection;
        this.resultSet = resultSet;
        this.index = index;
        this.field = field;
    }

    public SimpleJdbcValueAccess(JdbcValueSetter jdbcValueSetter, Connection connection, PreparedStatement statement,
            int index, Field field) throws SQLException {
        this.jdbcValueSetter = jdbcValueSetter;
        this.connection = connection;
        this.statement = statement;
        this.index = index;
        this.field = field;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public Field getField() {
        return field.toField();
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public T getValue(Map<String, Object> options) throws SQLException {
        if (jdbcValueGetter == null) {
            throw new JdbcValueAccessException("Get value is not supported");
        }
        return jdbcValueGetter.getValue(resultSet, connection, index, field, options);
    }

    @Override
    public <X> X getValue(Class<X> valueClass, Map<String, Object> options) throws SQLException {
        if (jdbcValueGetter == null) {
            throw new JdbcValueAccessException("Get value is not supported");
        }
        return jdbcValueGetter.getValue(resultSet, connection, index, field, valueClass, options);
    }

    @Override
    public <X> void setValue(X value, Map<String, Object> options) throws SQLException {
        if (jdbcValueSetter == null) {
            throw new JdbcValueAccessException("Set value is not supported");
        }
        jdbcValueSetter.setValue(statement, connection, index, field, value, options);
    }

}