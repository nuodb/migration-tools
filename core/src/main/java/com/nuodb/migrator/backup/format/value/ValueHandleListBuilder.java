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
package com.nuodb.migrator.backup.format.value;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.model.Column;
import com.nuodb.migrator.jdbc.model.SimpleColumn;
import com.nuodb.migrator.jdbc.model.SimpleColumnList;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.jdbc.type.JdbcValueAccess;
import com.nuodb.migrator.jdbc.type.jdbc2.JdbcDateTypeBase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.collect.Maps.newHashMap;

/**
 * @author Sergey Bushik
 */
public abstract class ValueHandleListBuilder {

    private Dialect dialect;

    private TimeZone timeZone;

    private Collection<? extends Column> columns;

    private ValueFormatRegistry valueFormatRegistry;

    public static ValueHandleListBuilder newBuilder(final ResultSet resultSet) {
        return new ValueHandleListBuilder() {
            private int column = 1;

            @Override
            protected JdbcValueAccess buildJdbcValueAccess(ValueHandle valueHandle) {
                return getDialect().getJdbcValueAccessProvider().getJdbcValueGetter(
                        resultSet, column++, valueHandle);
            }
        };
    }

    public static ValueHandleListBuilder newBuilder(final PreparedStatement statement) {
        return new ValueHandleListBuilder() {
            private int column = 1;

            @Override
            protected JdbcValueAccess buildJdbcValueAccess(ValueHandle valueHandle) {
                return getDialect().getJdbcValueAccessProvider().getJdbcValueGetter(
                        statement, column++, valueHandle);
            }
        };
    }

    public ValueHandleList build() {
        ValueHandleList valueHandleList = createValueHandleList();
        for (Column column : getColumns()) {
            ValueHandle valueHandle = createValueHandle(column);
            initValueHandle(valueHandle);
            valueHandleList.add(valueHandle);
        }
        return valueHandleList;
    }

    protected ValueHandleList createValueHandleList() {
        return new SimpleValueHandleList();
    }

    protected ValueHandle createValueHandle(Column column) {
        ValueHandle valueHandle = new SimpleValueHandle(column);
        JdbcTypeDesc jdbcTypeDesc = getDialect().getJdbcTypeRegistry().getJdbcTypeAlias(
                column.getTypeCode(), column.getTypeName());
        valueHandle.setTypeCode(jdbcTypeDesc.getTypeCode());
        valueHandle.setTypeName(jdbcTypeDesc.getTypeName());
        return valueHandle;
    }

    protected void initValueHandle(ValueHandle valueHandle) {
        initValueFormat(valueHandle);
        initValueType(valueHandle);
        initJdbcValueAccess(valueHandle);
        initJdbcValueAccessOptions(valueHandle);
    }

    protected void initValueFormat(ValueHandle valueHandle) {
        valueHandle.setValueFormat(buildValueFormat(valueHandle));
    }

    protected ValueFormat buildValueFormat(ValueHandle valueHandle) {
        return getValueFormatRegistry().getValueFormat(
                new JdbcTypeDesc(valueHandle.getTypeCode(), valueHandle.getTypeName()));
    }

    protected void initValueType(ValueHandle valueHandle) {
        valueHandle.setValueType(buildValueFormat(valueHandle).getValueType(valueHandle));
    }

    protected void initJdbcValueAccess(ValueHandle valueHandle) {
        valueHandle.setJdbcValueAccess(buildJdbcValueAccess(valueHandle));
    }

    protected abstract JdbcValueAccess buildJdbcValueAccess(ValueHandle valueHandle);

    protected void initJdbcValueAccessOptions(ValueHandle valueHandle) {
        valueHandle.setJdbcValueAccessOptions(buildJdbcValueAccessOptions(valueHandle));
    }

    protected Map<String, Object> buildJdbcValueAccessOptions(ValueHandle valueHandle) {
        Map<String, Object> jdbcValueAccessOptions = null;
        switch (valueHandle.getTypeCode()) {
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                if (dialect.supportsStatementWithTimezone()) {
                    jdbcValueAccessOptions = newHashMap();
                    jdbcValueAccessOptions.put(JdbcDateTypeBase.TIMEZONE, getTimeZone());
                }
                break;
            default:
        }
        return jdbcValueAccessOptions;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public ValueHandleListBuilder withDialect(Dialect dialect) {
        this.dialect = dialect;
        return this;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public ValueHandleListBuilder withTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public Collection<? extends Column> getColumns() {
        return columns;
    }

    public ValueHandleListBuilder withColumns(Collection<? extends Column> columns) {
        this.columns = columns;
        return this;
    }

    public ValueFormatRegistry getValueFormatRegistry() {
        return valueFormatRegistry;
    }

    public ValueHandleListBuilder withValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        this.valueFormatRegistry = valueFormatRegistry;
        return this;
    }

    static class SimpleValueHandle extends SimpleColumn implements ValueHandle {

        private ValueType valueType;
        private ValueFormat valueFormat;
        private JdbcValueAccess jdbcValueAccess;
        private Map<String, Object> jdbcValueAccessOptions;

        public SimpleValueHandle() {
        }

        public SimpleValueHandle(Column column) {
            super(column);
        }

        @Override
        public ValueType getValueType() {
            return valueType;
        }

        @Override
        public void setValueType(ValueType valueType) {
            this.valueType = valueType;
        }

        @Override
        public ValueFormat getValueFormat() {
            return valueFormat;
        }

        @Override
        public void setValueFormat(ValueFormat valueFormat) {
            this.valueFormat = valueFormat;
        }

        @Override
        public JdbcValueAccess getJdbcValueAccess() {
            return jdbcValueAccess;
        }

        @Override
        public void setJdbcValueAccess(JdbcValueAccess jdbcValueAccess) {
            this.jdbcValueAccess = jdbcValueAccess;
        }

        @Override
        public Map<String, Object> getJdbcValueAccessOptions() {
            return jdbcValueAccessOptions;
        }

        @Override
        public void setJdbcValueAccessOptions(Map<String, Object> jdbcValueAccessOptions) {
            this.jdbcValueAccessOptions = jdbcValueAccessOptions;
        }
    }

    static class SimpleValueHandleList extends SimpleColumnList<ValueHandle> implements ValueHandleList {
    }
}
