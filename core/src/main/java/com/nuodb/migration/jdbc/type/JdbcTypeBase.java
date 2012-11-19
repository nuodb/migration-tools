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
package com.nuodb.migration.jdbc.type;


import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public abstract class JdbcTypeBase<T> implements JdbcType<T> {

    private final JdbcTypeDesc typeDesc;
    private final Class<? extends T> typeClass;

    protected JdbcTypeBase(int typeCode, Class<? extends T> typeClass) {
        this(new JdbcTypeDesc(typeCode), typeClass);
    }

    protected JdbcTypeBase(int typeCode, String typeName, Class<? extends T> typeClass) {
        this(new JdbcTypeDesc(typeCode, typeName), typeClass);
    }

    protected JdbcTypeBase(JdbcTypeDesc typeDesc, Class<? extends T> typeClass) {
        this.typeDesc = typeDesc;
        this.typeClass = typeClass;
    }

    protected final int getTypeCode() {
        return typeDesc.getTypeCode();
    }

    protected final String getTypeName() {
        return typeDesc.getTypeName();
    }

    @Override
    public JdbcTypeDesc getTypeDesc() {
        return typeDesc;
    }

    @Override
    public Class<? extends T> getTypeClass() {
        return typeClass;
    }

    @Override
    public void setValue(PreparedStatement statement, int column, T value) throws SQLException {
        if (value == null) {
            setNullValue(statement, column);
        } else {
            setNullSafeValue(statement, value, column);
        }
    }

    protected void setNullValue(PreparedStatement statement, int column) throws SQLException {
        statement.setNull(column, getTypeCode());
    }

    protected abstract void setNullSafeValue(PreparedStatement statement, T value, int column) throws SQLException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JdbcTypeBase that = (JdbcTypeBase) o;

        if (typeDesc != null ? !typeDesc.equals(that.typeDesc) : that.typeDesc != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return typeDesc != null ? typeDesc.hashCode() : 0;
    }

    @Override
    public String toString() {
        return typeDesc.toString();
    }
}
