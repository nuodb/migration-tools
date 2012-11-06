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
package com.nuodb.migration.result.format.jdbc;

import com.nuodb.migration.jdbc.metamodel.ValueModel;
import com.nuodb.migration.jdbc.type.JdbcTypeGet;
import com.nuodb.migration.jdbc.type.JdbcTypeSet;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeValueAccessorImpl<T> implements JdbcTypeValueAccessor<T> {

    private JdbcTypeGet<T> jdbcTypeGet;
    private ResultSet resultSet;
    private JdbcTypeSet<T> jdbcTypeSet;
    private PreparedStatement preparedStatement;
    private ValueModel valueModel;
    private int column;

    public JdbcTypeValueAccessorImpl(JdbcTypeGet<T> jdbcTypeGet, ResultSet resultSet,
                                     int column, ValueModel valueModel) {
        this.jdbcTypeGet = jdbcTypeGet;
        this.resultSet = resultSet;
        this.valueModel = valueModel;
        this.column = column;
    }

    public JdbcTypeValueAccessorImpl(JdbcTypeSet<T> jdbcTypeSet, PreparedStatement preparedStatement,
                                     int column, ValueModel valueModel) {
        this.jdbcTypeSet = jdbcTypeSet;
        this.preparedStatement = preparedStatement;
        this.valueModel = valueModel;
        this.column = column;
    }

    @Override
    public T getValue() throws SQLException {
        if (jdbcTypeGet == null) {
            throw new JdbcTypeValueException("Get value is unsupported");
        }
        return jdbcTypeGet.getValue(resultSet, column);
    }

    @Override
    public <X> X getValue(Class<X> valueClass) throws SQLException {
        if (jdbcTypeGet == null) {
            throw new JdbcTypeValueException("Get value is unsupported");
        }
        return jdbcTypeGet.getValue(resultSet, column, valueClass);
    }

    @Override
    public <X> void setValue(X value) throws SQLException {
        if (jdbcTypeSet == null) {
            throw new JdbcTypeValueException("Set value is unsupported");
        }
        jdbcTypeSet.setValue(preparedStatement, column, value);
    }

    @Override
    public ValueModel getValueModel() {
        return valueModel;
    }
}
