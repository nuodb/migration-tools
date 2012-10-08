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

import com.nuodb.tools.migration.MigrationException;
import com.nuodb.tools.migration.jdbc.metamodel.ResultSetMetaModel;
import com.nuodb.tools.migration.jdbc.type.BitTypeExtractor;
import com.nuodb.tools.migration.jdbc.type.IntegerTypeExtractor;
import com.nuodb.tools.migration.jdbc.type.LongVarcharTypeExtractor;
import com.nuodb.tools.migration.jdbc.type.TypeExtractorLookup;
import com.nuodb.tools.migration.jdbc.type.ValueAcceptor;
import com.nuodb.tools.migration.jdbc.type.ValueExtractor;
import com.nuodb.tools.migration.jdbc.type.VarcharTypeExtractor;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class CsvOutputFormat implements OutputFormat {

    private ResultSetMetaModel metaModel;

    private CsvListWriter csvWriter;
    private TypeExtractorLookup typeExtractorLookup;

    @Override
    public void open(Writer writer) {
        this.csvWriter = new CsvListWriter(writer, CsvPreference.STANDARD_PREFERENCE);
    }

    @Override
    public void open(OutputStream output) {
        this.csvWriter = new CsvListWriter(new PrintWriter(output), CsvPreference.STANDARD_PREFERENCE);
    }

    @Override
    public void init(ResultSet resultSet) throws SQLException {
        this.metaModel = new ResultSetMetaModel(resultSet);
        try {
            this.csvWriter.writeHeader(metaModel.getColumns());
        } catch (IOException e) {
            handleIOException(e);
        }
        this.typeExtractorLookup = createTypeExtractorLookup();
    }

    protected TypeExtractorLookup createTypeExtractorLookup() {
        TypeExtractorLookup typeExtractorLookup = new TypeExtractorLookup();
        typeExtractorLookup.register(new VarcharTypeExtractor());
        typeExtractorLookup.register(new LongVarcharTypeExtractor());
        typeExtractorLookup.register(new BitTypeExtractor());
        typeExtractorLookup.register(new IntegerTypeExtractor());
        return typeExtractorLookup;
    }

    @Override
    public void row(ResultSet resultSet) throws SQLException {
        int count = metaModel.getColumnCount();
        String[] columns = new String[count];
        CsvValueAcceptor acceptor = new CsvValueAcceptor();
        for (int index = 0; index < count; index++) {
            final int type = metaModel.getColumnType(index);
            ValueExtractor<Object> extractor = (ValueExtractor<Object>) typeExtractorLookup.lookup(type);
            if (extractor == null) {
                throw new MigrationException(String.format("Type extractor for java.sql.Types type %1$d not implemented/registered", type));
            }
            int column = index + 1;
            extractor.extract(resultSet, column, acceptor);
            columns[index] = acceptor.getValue();
        }
        try {
            csvWriter.write(columns);
        } catch (IOException e) {
            handleIOException(e);
        }
    }

    @Override
    public void end() {
        try {
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            handleIOException(e);
        }
    }

    protected void handleIOException(IOException e) {
        throw new OutputFormatException(e);
    }

    class CsvValueAcceptor implements ValueAcceptor<Object> {

        private String value;

        @Override
        public void accept(Object value, int type) {
            switch (type) {
                case Types.BIT:
                case Types.INTEGER:
                case Types.VARCHAR:
                    this.value = value != null ? value.toString() : "";
                    break;
                default:
                    throw new MigrationException(String.format("Type unhandled for type %1$d not implemented/registered", type));
            }
        }

        public String getValue() {
            return value;
        }
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
}
