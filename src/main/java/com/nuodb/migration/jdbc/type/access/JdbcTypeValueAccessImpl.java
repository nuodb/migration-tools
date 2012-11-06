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
package com.nuodb.migration.jdbc.type.access;

import com.nuodb.migration.jdbc.metamodel.ValueModel;
import com.nuodb.migration.jdbc.type.JdbcType;
import com.nuodb.migration.jdbc.type.JdbcTypeAdapter;
import com.nuodb.migration.jdbc.type.JdbcTypeException;
import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migration.jdbc.type.JdbcTypeRegistryBase;
import com.nuodb.migration.jdbc.type.jdbc4.Jdbc4TypeRegistry;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.nuodb.migration.jdbc.metamodel.ValueModelFactory.createValueModel;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcTypeValueAccessImpl extends JdbcTypeRegistryBase implements JdbcTypeValueAccess {

    public JdbcTypeValueAccessImpl() {
        this(Jdbc4TypeRegistry.INSTANCE);
    }

    public JdbcTypeValueAccessImpl(JdbcTypeRegistry jdbcTypeRegistry) {
        super(jdbcTypeRegistry);
    }

    @Override
    public <T> JdbcTypeValueGetter<T> createValueGetter(int typeCode) {
        return createValueGetter(getJdbcTypeRequired(typeCode));
    }

    @Override
    public <T> JdbcTypeValueSetter<T> createValueSetter(int typeCode) {
        return createValueSetter(getJdbcTypeRequired(typeCode));
    }

    @Override
    public <T> JdbcTypeValueGetter<T> createValueGetter(JdbcType<T> jdbcType) {
        return new JdbcTypeValueGetterImpl<T>(jdbcType);
    }

    @Override
    public <T> JdbcTypeValueSetter<T> createValueSetter(JdbcType<T> jdbcType) {
        return new JdbcTypeValueSetterImpl<T>(jdbcType);
    }

    @Override
    public <T> JdbcTypeValueAccessor<T> createResultSetAccessor(ResultSet resultSet, int column) {
        try {
            return createResultSetAccessor(resultSet, column, createValueModel(resultSet, column));
        } catch (SQLException exception) {
            throw new JdbcTypeException(exception);
        }
    }

    @Override
    public <T> JdbcTypeValueAccessor<T> createResultSetAccessor(ResultSet resultSet, int column,
                                                                ValueModel valueModel) {
        return new JdbcTypeValueAccessorImpl<T>(
                (JdbcTypeValueGetter<T>) createValueGetter(valueModel.getTypeCode()), resultSet, column, valueModel);
    }

    @Override
    public <T> JdbcTypeValueAccessor<T> createStatementAccessor(PreparedStatement statement, int column) {
        try {
            return createStatementAccessor(statement, column, createValueModel(statement.getMetaData(), column));
        } catch (SQLException exception) {
            throw new JdbcTypeException(exception);
        }
    }

    @Override
    public <T> JdbcTypeValueAccessor<T> createStatementAccessor(PreparedStatement statement, int column,
                                                                ValueModel valueModel) {
        return new JdbcTypeValueAccessorImpl<T>(
                (JdbcTypeValueSetter<T>) createValueSetter(valueModel.getTypeCode()), statement, column, valueModel);
    }

    protected <T> JdbcTypeAdapter<T> getJdbcTypeAdapter(Class valueClass, Class typeClass) {
        JdbcTypeAdapter jdbcTypeAdapter = null;
        if (valueClass != null && !valueClass.isAssignableFrom(typeClass)) {
            jdbcTypeAdapter = getJdbcTypeAdapter(typeClass);
            if (jdbcTypeAdapter == null) {
                throw new JdbcTypeException(
                        String.format("Jdbc type %s adapter not found to adapt %s type",
                                typeClass.getName(), valueClass.getName()));
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

    class JdbcTypeValueGetterImpl<T> implements JdbcTypeValueGetter<T> {

        private JdbcType<T> jdbcType;

        public JdbcTypeValueGetterImpl(JdbcType<T> jdbcType) {
            this.jdbcType = jdbcType;
        }

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
            X value = (X) jdbcType.getValue(resultSet, column);
            JdbcTypeAdapter<X> adapter = getJdbcTypeAdapter(valueClass, jdbcType.getTypeClass());
            if (adapter != null) {
                Statement statement = resultSet.getStatement();
                value = adapter.unwrap(value, valueClass, statement.getConnection());
            }
            return value;
        }
    }

    class JdbcTypeValueSetterImpl<T> implements JdbcTypeValueSetter<T> {

        private JdbcType<T> jdbcType;

        public JdbcTypeValueSetterImpl(JdbcType<T> jdbcType) {
            this.jdbcType = jdbcType;
        }

        @Override
        public JdbcType<T> getJdbcType() {
            return jdbcType;
        }

        @Override
        public <X> void setValue(PreparedStatement statement, int column, X value) throws SQLException {
            JdbcTypeAdapter<X> adapter = getJdbcTypeAdapter(value != null ? value.getClass() : null,
                    jdbcType.getTypeClass());
            if (adapter != null) {
                value = adapter.wrap(value, statement.getConnection());
            }
            jdbcType.setValue(statement, column, (T) value);
        }
    }

    class JdbcTypeValueAccessorImpl<T> implements JdbcTypeValueAccessor<T> {

        private JdbcTypeValueGetter<T> jdbcTypeValueGetter;
        private ResultSet resultSet;
        private JdbcTypeValueSetter<T> jdbcTypeValueSetter;
        private PreparedStatement statement;
        private ValueModel valueModel;
        private int column;

        public JdbcTypeValueAccessorImpl(JdbcTypeValueGetter<T> jdbcTypeValueGetter, ResultSet resultSet, int column,
                                         ValueModel valueModel) {
            this.jdbcTypeValueGetter = jdbcTypeValueGetter;
            this.resultSet = resultSet;
            this.valueModel = valueModel;
            this.column = column;
        }

        public JdbcTypeValueAccessorImpl(JdbcTypeValueSetter<T> jdbcTypeValueSetter, PreparedStatement statement,
                                         int column, ValueModel valueModel) {
            this.jdbcTypeValueSetter = jdbcTypeValueSetter;
            this.statement = statement;
            this.valueModel = valueModel;
            this.column = column;
        }

        @Override
        public T getValue() throws SQLException {
            if (jdbcTypeValueGetter == null) {
                throw new JdbcTypeException("Get value is unsupported");
            }
            return jdbcTypeValueGetter.getValue(resultSet, column);
        }

        @Override
        public <X> X getValue(Class<X> valueClass) throws SQLException {
            if (jdbcTypeValueGetter == null) {
                throw new JdbcTypeException("Get value is unsupported");
            }
            return jdbcTypeValueGetter.getValue(resultSet, column, valueClass);
        }

        @Override
        public <X> void setValue(X value) throws SQLException {
            if (jdbcTypeValueSetter == null) {
                throw new JdbcTypeException("Set value is unsupported");
            }
            jdbcTypeValueSetter.setValue(statement, column, value);
        }

        @Override
        public ValueModel getValueModel() {
            return valueModel;
        }
    }

}
