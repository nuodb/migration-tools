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
package com.nuodb.tools.migration.dump.output;

import com.nuodb.tools.migration.dump.DumpException;
import com.nuodb.tools.migration.jdbc.metamodel.ResultSetMetaModel;
import com.nuodb.tools.migration.jdbc.type.extract.JdbcTypeAcceptor;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class CsvDumpOutput extends DumpOutputBase {

    private CsvListWriter output;
    private ResultSetMetaModel resultSetMetaModel;

    @Override
    public void init() {
        Writer writer = getWriter();
        OutputStream outputStream = getOutputStream();
        // TODO: process extra getAttributes() and configure CSV preference
        CsvPreference preference = CsvPreference.STANDARD_PREFERENCE;
        if (writer != null) {
            output = new CsvListWriter(writer, preference);
        } else if (outputStream != null) {
            output = new CsvListWriter(new PrintWriter(outputStream), preference);
        }
    }

    @Override
    public void dumpBegin(ResultSet resultSet) throws IOException, SQLException {
        resultSetMetaModel = new ResultSetMetaModel(resultSet);
        output.writeHeader(resultSetMetaModel.getColumns());
    }

    @Override
    public void dumpRow(ResultSet resultSet) throws IOException, SQLException {
        final List<String> columns = new ArrayList<String>();
        for (int column = 0; column < resultSetMetaModel.getColumnCount(); column++) {
            getJdbcTypeExtractor().extract(resultSet, column + 1, new JdbcTypeAcceptor() {
                @Override
                public void accept(Object value, int type) throws SQLException {
                    columns.add(format(value, type));
                }
            });
        }
        output.write(columns);
    }

//    private static String getColumnValue(ResultSet rs, int colType, int colIndex)
//            throws SQLException {
//
//        String value = "";
//
//        switch (colType) {
//            case Types.BIT:
//                Object bit = rs.getObject(colIndex);
//                if (bit != null) {
//                    value = String.valueOf(bit);
//                }
//                break;
//            case Types.BOOLEAN:
//                boolean b = rs.getBoolean(colIndex);
//                if (!rs.wasNull()) {
//                    value = Boolean.valueOf(b).toString();
//                }
//                break;
//            case Types.CLOB:
//                Clob c = rs.getClob(colIndex);
//                if (c != null) {
//                    value = read(c);
//                }
//                break;
//            case Types.BIGINT:
//                long lv = rs.getLong(colIndex);
//                if (!rs.wasNull()) {
//                    value = Long.toString(lv);
//                }
//                break;
//            case Types.DECIMAL:
//            case Types.DOUBLE:
//            case Types.FLOAT:
//            case Types.REAL:
//            case Types.NUMERIC:
//                BigDecimal bd = rs.getBigDecimal(colIndex);
//                if (bd != null) {
//                    value = bd.toString();
//                }
//                break;
//            case Types.INTEGER:
//            case Types.TINYINT:
//            case Types.SMALLINT:
//                int intValue = rs.getInt(colIndex);
//                if (!rs.wasNull()) {
//                    value = Integer.toString(intValue);
//                }
//                break;
//            case Types.JAVA_OBJECT:
//                Object obj = rs.getObject(colIndex);
//                if (obj != null) {
//                    value = String.valueOf(obj);
//                }
//                break;
//            case Types.DATE:
//                java.sql.Date date = rs.getDate(colIndex);
//                if (date != null) {
//                    SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
//                    value = dateFormat.format(date);
//                }
//                break;
//            case Types.TIME:
//                Time t = rs.getTime(colIndex);
//                if (t != null) {
//                    value = t.toString();
//                }
//                break;
//            case Types.TIMESTAMP:
//                Timestamp tstamp = rs.getTimestamp(colIndex);
//                if (tstamp != null) {
//                    SimpleDateFormat timeFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
//                    value = timeFormat.format(tstamp);
//                }
//                break;
//            case Types.LONGVARCHAR:
//            case Types.VARCHAR:
//            case Types.CHAR:
//                value = rs.getString(colIndex);
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
//
//    private static String read(Clob c) throws SQLException {
//        StringBuilder sb = new StringBuilder((int) c.length());
//        Reader r = c.getCharacterStream();
//        char[] cbuf = new char[2048];
//        int n = 0;
//        while ((n = r.read(cbuf, 0, cbuf.length)) != -1) {
//            if (n > 0) {
//                sb.append(cbuf, 0, n);
//            }
//        }
//        return sb.toString();
//    }

    protected String format(Object value, int type) {
        String result;
        switch (type) {
            case Types.BIT:
            case Types.INTEGER:
            case Types.VARCHAR:
                result = value != null ? value.toString() : "";
                break;
            default:
                throw new DumpException(String.format("Jdbc type %1$d is unsupported", type));
        }
        return result;
    }

    @Override
    public void dumpEnd(ResultSet resultSet) throws IOException {
        output.flush();
        output.close();
    }
}
