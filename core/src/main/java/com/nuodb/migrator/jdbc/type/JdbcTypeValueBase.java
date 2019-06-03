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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public abstract class JdbcTypeValueBase<T> implements JdbcTypeValue<T> {

    private final JdbcTypeDesc jdbcTypeDesc;
    private final Class<? extends T> valueClass;

    protected JdbcTypeValueBase(int typeCode, Class<? extends T> valueClass) {
        this(new JdbcTypeDesc(typeCode), valueClass);
    }

    protected JdbcTypeValueBase(int typeCode, String typeName, Class<? extends T> valueClass) {
        this(new JdbcTypeDesc(typeCode, typeName), valueClass);
    }

    protected JdbcTypeValueBase(JdbcTypeDesc jdbcTypeDesc, Class<? extends T> valueClass) {
        this.jdbcTypeDesc = jdbcTypeDesc;
        this.valueClass = valueClass;
    }

    public int getTypeCode() {
        return jdbcTypeDesc.getTypeCode();
    }

    public String getTypeName() {
        return jdbcTypeDesc.getTypeName();
    }

    @Override
    public JdbcTypeDesc getJdbcTypeDesc() {
        return jdbcTypeDesc;
    }

    @Override
    public Class<? extends T> getValueClass() {
        return valueClass;
    }

    @Override
    public void setValue(PreparedStatement statement, int index, Field field, T value, Map<String, Object> options)
            throws SQLException {
        if (value == null) {
            setNullValue(statement, index, field);
        } else {
            setNullSafeValue(statement, value, index, field, options);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T getOption(Map<String, Object> options, String option) {
        return options != null ? (T) options.get(option) : null;
    }

    protected void setNullValue(PreparedStatement statement, int index, Field field) throws SQLException {
        statement.setNull(index, jdbcTypeDesc.getTypeCode());
    }

    protected abstract void setNullSafeValue(PreparedStatement statement, T value, int index, Field field,
            Map<String, Object> options) throws SQLException;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        JdbcTypeValueBase that = (JdbcTypeValueBase) o;

        if (jdbcTypeDesc != null ? !jdbcTypeDesc.equals(that.jdbcTypeDesc) : that.jdbcTypeDesc != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return jdbcTypeDesc != null ? jdbcTypeDesc.hashCode() : 0;
    }

    @Override
    public String toString() {
        return jdbcTypeDesc.toString();
    }
}
