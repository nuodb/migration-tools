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
package com.nuodb.tools.migration.result.format;

import com.nuodb.tools.migration.jdbc.type.JdbcType;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeGet;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeSet;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeValueImpl<T> implements JdbcTypeValue<T> {

    private JdbcType<T> jdbcType;
    private JdbcTypeGet<T> jdbcTypeGet;
    private ResultSet resultSet;
    private JdbcTypeSet<T> jdbcTypeSet;
    private PreparedStatement preparedStatement;
    private int column;

    public JdbcTypeValueImpl(JdbcTypeGet<T> jdbcTypeGet, ResultSet resultSet, int column) {
        this.jdbcType = jdbcTypeGet.getJdbcType();
        this.jdbcTypeGet = jdbcTypeGet;
        this.resultSet = resultSet;
        this.column = column;
    }

    public JdbcTypeValueImpl(JdbcTypeSet<T> jdbcTypeSet, PreparedStatement preparedStatement, int column) {
        this.jdbcType = jdbcTypeSet.getJdbcType();
        this.jdbcTypeSet = jdbcTypeSet;
        this.preparedStatement = preparedStatement;
        this.column = column;
    }

    @Override
    public int getColumn() {
        return column;
    }

    @Override
    public T getValue() throws SQLException {
        if (jdbcTypeGet == null) {
            throw new ResultInputException("Get value is unsupported");
        }
        return jdbcTypeGet.getValue(resultSet, column);
    }

    @Override
    public <X> X getValue(Class<X> valueClass) throws SQLException {
        if (jdbcTypeGet == null) {
            throw new ResultInputException("Get value is unsupported");
        }
        return jdbcTypeGet.getValue(resultSet, column, valueClass);
    }

    @Override
    public <X> void setValue(X value) throws SQLException {
        if (jdbcTypeSet == null) {
            throw new ResultInputException("Set value is unsupported");
        }
        jdbcTypeSet.setValue(preparedStatement, column, value);
    }

    @Override
    public JdbcType<T> getJdbcType() {
        return jdbcType;
    }
}
