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
package com.nuodb.tools.migration.jdbc.type.extract;

import com.nuodb.tools.migration.MigrationException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author Sergey Bushik
 */
public class Jdbc2TypeExtractor extends JdbcTypeExtractorBase {

    @Override
    protected void extract(ResultSet resultSet, int column, int columnType, JdbcTypeAcceptor acceptor) throws SQLException {
        switch (columnType) {
            case Types.BIT:
                boolean booleanValue = resultSet.getBoolean(column);
                acceptor.accept(resultSet.wasNull() ? null : booleanValue, columnType);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                short shortValue = resultSet.getShort(column);
                acceptor.accept(resultSet.wasNull() ? null : shortValue, columnType);
                break;
            case Types.INTEGER:
                int intValue = resultSet.getInt(column);
                acceptor.accept(resultSet.wasNull() ? null : intValue, columnType);
                break;
            case Types.BIGINT:
                long longValue = resultSet.getLong(column);
                acceptor.accept(resultSet.wasNull() ? null : longValue, columnType);
                break;
            case Types.FLOAT:
            case Types.REAL:
                float floatValue = resultSet.getFloat(column);
                acceptor.accept(resultSet.wasNull() ? null : floatValue, columnType);
                break;
            case Types.DOUBLE:
                double doubleValue = resultSet.getDouble(column);
                acceptor.accept(resultSet.wasNull() ? null : doubleValue, columnType);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                acceptor.accept(resultSet.getBigDecimal(column), columnType);
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                String stringValue = resultSet.getString(column);
                acceptor.accept(resultSet.wasNull() ? null : stringValue, columnType);
                break;
            case Types.DATE:
                acceptor.accept(resultSet.getDate(column), columnType);
                break;
            case Types.TIME:
                acceptor.accept(resultSet.getTime(column), columnType);
                break;
            case Types.TIMESTAMP:
                acceptor.accept(resultSet.getTimestamp(column), columnType);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                acceptor.accept(resultSet.getBytes(column), columnType);
                break;
            case Types.NULL:
                acceptor.accept(null, columnType);
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                acceptor.accept(resultSet.getObject(column), columnType);
                break;
            case Types.ARRAY:
                acceptor.accept(resultSet.getArray(column), columnType);
                break;
            case Types.BLOB:
                acceptor.accept(resultSet.getBlob(column), columnType);
                break;
            case Types.CLOB:
                acceptor.accept(resultSet.getClob(column), columnType);
                break;
            case Types.REF:
                acceptor.accept(resultSet.getRef(column), columnType);
                break;
            default:
                throw new MigrationException(String.format("Jdbc type %1$d is unsupported", columnType));
        }
    }
}
