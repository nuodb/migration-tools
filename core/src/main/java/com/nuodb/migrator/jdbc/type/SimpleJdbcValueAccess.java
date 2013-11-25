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
package com.nuodb.migrator.jdbc.type;

import com.nuodb.migrator.jdbc.connection.ConnectionProxy;
import com.nuodb.migrator.jdbc.model.Column;

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
    private PreparedStatement preparedStatement;
    private Connection connection;
    private int columnIndex;
    private Column column;

    public SimpleJdbcValueAccess(JdbcValueGetter<T> jdbcValueGetter, ResultSet resultSet,
                                 int columnIndex, Column column) throws SQLException {
        this.jdbcValueGetter = jdbcValueGetter;
        this.resultSet = resultSet;
        this.connection = getConnection(resultSet.getStatement().getConnection());
        this.columnIndex = columnIndex;
        this.column = column;
    }

    public SimpleJdbcValueAccess(JdbcValueSetter jdbcValueSetter, PreparedStatement preparedStatement,
                                 int columnIndex, Column column) throws SQLException {
        this.jdbcValueSetter = jdbcValueSetter;
        this.preparedStatement = preparedStatement;
        this.connection = getConnection(preparedStatement.getConnection());
        this.columnIndex = columnIndex;
        this.column = column;
    }

    protected Connection getConnection(Connection connection) {
        if (connection instanceof ConnectionProxy) {
            return ((ConnectionProxy) connection).getConnection();
        } else {
            return connection;
        }
    }

    @Override
    public Column getColumn() {
        return column.asColumn();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public T getValue(Map<String, Object> options) throws SQLException {
        if (jdbcValueGetter == null) {
            throw new JdbcValueAccessException("Get value is not supported");
        }
        return jdbcValueGetter.getValue(resultSet, connection, columnIndex, column, options);
    }

    @Override
    public <X> X getValue(Class<X> valueClass, Map<String, Object> options) throws SQLException {
        if (jdbcValueGetter == null) {
            throw new JdbcValueAccessException("Get value is not supported");
        }
        return jdbcValueGetter.getValue(resultSet, connection, columnIndex, column, valueClass, options);
    }

    @Override
    public <X> void setValue(X value, Map<String, Object> options) throws SQLException {
        if (jdbcValueSetter == null) {
            throw new JdbcValueAccessException("Set value is not supported");
        }
        jdbcValueSetter.setValue(preparedStatement, connection, columnIndex, column, value, options);
    }

}