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
package com.nuodb.tools.migration.jdbc.type;

import com.nuodb.tools.migration.jdbc.type.jdbc4.Jdbc4TypeRegistry;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcTypeAccessorImpl extends JdbcTypeRegistryBase implements JdbcTypeAccessor {

    public JdbcTypeAccessorImpl() {
        this(Jdbc4TypeRegistry.INSTANCE);
    }

    public JdbcTypeAccessorImpl(JdbcTypeRegistry jdbcTypeRegistry) {
        super(jdbcTypeRegistry);
    }

    @Override
    public <T> JdbcTypeGet<T> getJdbcTypeGet(int typeCode) {
        return getJdbcTypeGet(getJdbcTypeRequired(typeCode));
    }

    @Override
    public <T> JdbcTypeGet<T> getJdbcTypeGet(final JdbcType<T> jdbcType) {
        return new JdbcTypeGet<T>() {
            @Override
            public JdbcType<T> getJdbcType() {
                return jdbcType;
            }

            @Override
            public T getValue(ResultSet resultSet, int column) throws SQLException {
                return jdbcType.getValue(resultSet, column);
            }

            @Override
            public <X> X getValue(ResultSet resultSet, int column, Class<X> valueClass) throws SQLException {
                JdbcTypeAdapter<X> jdbcTypeAdapter = getJdbcTypeAdapter(valueClass, jdbcType.getTypeClass());
                X value = (X) jdbcType.getValue(resultSet, column);
                if (jdbcTypeAdapter != null) {
                    Statement statement = resultSet.getStatement();
                    value = jdbcTypeAdapter.unwrap(value, valueClass, statement.getConnection());
                }
                return value;
            }
        };
    }

    @Override
    public <T> JdbcTypeSet<T> getJdbcTypeSet(int typeCode) {
        return getJdbcTypeSet(getJdbcTypeRequired(typeCode));
    }

    @Override
    public <T> JdbcTypeSet<T> getJdbcTypeSet(final JdbcType<T> jdbcType) {
        return new JdbcTypeSet<T>() {
            @Override
            public JdbcType<T> getJdbcType() {
                return jdbcType;
            }

            @Override
            public <X> void setValue(PreparedStatement statement, int column, X value) throws SQLException {
                JdbcTypeAdapter<X> jdbcTypeAdapter = getJdbcTypeAdapter(value != null ? value.getClass() : null,
                        jdbcType.getTypeClass());
                if (jdbcTypeAdapter != null) {
                    value = jdbcTypeAdapter.wrap(value, statement.getConnection());
                }
                jdbcType.setValue(statement, column, (T) value);
            }
        };
    }

    protected <T> JdbcTypeAdapter<T> getJdbcTypeAdapter(Class valueClass, Class typeClass) {
        JdbcTypeAdapter jdbcTypeAdapter = null;
        if (valueClass != null && !valueClass.isAssignableFrom(typeClass)) {
            jdbcTypeAdapter = getJdbcTypeAdapter(typeClass);
            if (jdbcTypeAdapter == null) {
                throw new JdbcTypeException(
                        String.format("Jdbc type %s adapter not found to adapt %s type", typeClass.getName(),
                                valueClass.getName()));
            }
        }
        return jdbcTypeAdapter;
    }

    protected JdbcType getJdbcTypeRequired(int typeCode) {
        JdbcType jdbcType = getJdbcType(typeCode);
        if (jdbcType == null) {
            throw new JdbcTypeException(
                    String.format("Jdbc type %s is not supported", getTypeName(typeCode)));
        }
        return jdbcType;
    }
}
