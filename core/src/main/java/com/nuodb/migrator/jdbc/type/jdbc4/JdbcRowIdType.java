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
package com.nuodb.migrator.jdbc.type.jdbc4;

import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeBase;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import java.sql.*;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class JdbcRowIdType extends JdbcTypeBase<RowId> {

    public static final JdbcType INSTANCE = new JdbcRowIdType();

    public JdbcRowIdType() {
        super(Types.ROWID, RowId.class);
    }

    public JdbcRowIdType(int typeCode) {
        super(typeCode, RowId.class);
    }

    public JdbcRowIdType(int typeCode, String typeName) {
        super(typeCode, typeName, RowId.class);
    }

    public JdbcRowIdType(JdbcTypeDesc typeDesc) {
        super(typeDesc, RowId.class);
    }

    @Override
    public RowId getValue(ResultSet resultSet, int column, Map<String, Object> options) throws SQLException {
        return resultSet.getRowId(column);
    }

    @Override
    protected void setNullSafeValue(PreparedStatement statement, RowId value, int column, Map<String, Object> options) throws SQLException {
        statement.setRowId(column, value);
    }
}