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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public interface ValueExtractor<T> {

    int getType();

    void extract(ResultSet resultSet, int column, ValueAcceptor<T> acceptor) throws SQLException;
}


//switch (columnType) {
//        case Types.BIT:
//        extractBit(resultSet, column, acceptor);
//break;
//case Types.BOOLEAN:
//        boolean b = resultSet.getBoolean(columnIndex);
//if (!resultSet.wasNull()) {
//        value = Boolean.valueOf(b).toString();
//}
//        break;
//case Types.CLOB:
//        Clob c = resultSet.getClob(columnIndex);
//if (c != null) {
//        value = read(c);
//}
//        break;
//case Types.BIGINT:
//        long lv = resultSet.getLong(columnIndex);
//if (!resultSet.wasNull()) {
//        value = Long.toString(lv);
//}
//        break;
//case Types.DECIMAL:
//        case Types.DOUBLE:
//        case Types.FLOAT:
//        case Types.REAL:
//        case Types.NUMERIC:
//        BigDecimal bd = resultSet.getBigDecimal(columnIndex);
//if (bd != null) {
//        value = bd.toString();
//}
//        break;
//case Types.INTEGER:
//        case Types.TINYINT:
//        case Types.SMALLINT:
//        int intValue = resultSet.getInt(columnIndex);
//if (!resultSet.wasNull()) {
//        value = Integer.toString(intValue);
//}
//        break;
//case Types.JAVA_OBJECT:
//        Object obj = resultSet.getObject(columnIndex);
//if (obj != null) {
//        value = String.valueOf(obj);
//}
//        break;
//case Types.DATE:
//        java.sql.Date date = resultSet.getDate(columnIndex);
//if (date != null) {
//        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
//value = dateFormat.format(date);
//}
//        break;
//case Types.TIME:
//        Time t = resultSet.getTime(columnIndex);
//if (t != null) {
//        value = t.toString();
//}
//        break;
//case Types.TIMESTAMP:
//        Timestamp tstamp = resultSet.getTimestamp(columnIndex);
//if (tstamp != null) {
//        SimpleDateFormat timeFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
//value = timeFormat.format(tstamp);
//}
//        break;
//case Types.LONGVARCHAR:
//        case Types.VARCHAR:
//        case Types.CHAR:
//        value = resultSet.getString(columnIndex);
//break;
//default:
//        value = "";
//}
//        }
//
//protected boolean extractBit(ResultSet resultSet, int column, ValueAcceptor acceptor) throws SQLException {
//        return acceptor.acceptBoolean();
//}

//        String value = "";
//
//        switch (columnType) {
//            case Types.BIT:
//
//                break;
//            case Types.BOOLEAN:
//                boolean b = resultSet.getBoolean(columnIndex);
//                if (!resultSet.wasNull()) {
//                    value = Boolean.valueOf(b).toString();
//                }
//                break;
//            case Types.CLOB:
//                Clob c = resultSet.getClob(columnIndex);
//                if (c != null) {
//                    value = read(c);
//                }
//                break;
//            case Types.BIGINT:
//                long lv = resultSet.getLong(columnIndex);
//                if (!resultSet.wasNull()) {
//                    value = Long.toString(lv);
//                }
//                break;
//            case Types.DECIMAL:
//            case Types.DOUBLE:
//            case Types.FLOAT:
//            case Types.REAL:
//            case Types.NUMERIC:
//                BigDecimal bd = resultSet.getBigDecimal(columnIndex);
//                if (bd != null) {
//                    value = bd.toString();
//                }
//                break;
//            case Types.INTEGER:
//            case Types.TINYINT:
//            case Types.SMALLINT:
//                int intValue = resultSet.getInt(columnIndex);
//                if (!resultSet.wasNull()) {
//                    value = Integer.toString(intValue);
//                }
//                break;
//            case Types.JAVA_OBJECT:
//                Object obj = resultSet.getObject(columnIndex);
//                if (obj != null) {
//                    value = String.valueOf(obj);
//                }
//                break;
//            case Types.DATE:
//                java.sql.Date date = resultSet.getDate(columnIndex);
//                if (date != null) {
//                    SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
//                    value = dateFormat.format(date);
//                }
//                break;
//            case Types.TIME:
//                Time t = resultSet.getTime(columnIndex);
//                if (t != null) {
//                    value = t.toString();
//                }
//                break;
//            case Types.TIMESTAMP:
//                Timestamp tstamp = resultSet.getTimestamp(columnIndex);
//                if (tstamp != null) {
//                    SimpleDateFormat timeFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
//                    value = timeFormat.format(tstamp);
//                }
//                break;
//            case Types.LONGVARCHAR:
//            case Types.VARCHAR:
//            case Types.CHAR:
//                value = resultSet.getString(columnIndex);
//                break;
//            default:
//                value = "";
//        }
//
//
//        if (value == null) {
//            value = "";
//        }
//
//        return value;
//
//    }
//}
