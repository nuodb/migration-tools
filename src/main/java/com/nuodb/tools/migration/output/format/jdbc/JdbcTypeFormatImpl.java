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
package com.nuodb.tools.migration.output.format.jdbc;

import com.nuodb.tools.migration.jdbc.type.JdbcType;
import org.apache.commons.codec.binary.Base64;

import javax.sql.rowset.serial.SerialRef;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static java.lang.String.valueOf;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcTypeFormatImpl extends JdbcTypeFormatBase<Object> {

    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd-MMM-yyyy");

    public static final DateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

    public static final DateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

    private DateFormat dateFormat = DATE_FORMAT;
    private DateFormat timeFormat = TIME_FORMAT;
    private DateFormat timestampFormat = TIMESTAMP_FORMAT;

    protected String doFormat(JdbcTypeValue jdbcTypeValue) throws Exception {
        JdbcType jdbcType = jdbcTypeValue.getJdbcType();
        Object jdbcValue;
        String value = null;
        switch (jdbcType.getTypeCode()) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                jdbcValue = jdbcTypeValue.getValue();
                if (jdbcValue != null) {
                    value = valueOf(jdbcValue);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                jdbcValue = jdbcTypeValue.getValue();
                if (jdbcValue != null) {
                    value = valueOf(jdbcValue);
                }
                break;
            case Types.DATE:
                jdbcValue = jdbcTypeValue.getValue();
                if (jdbcValue != null) {
                    value = getDateFormat().format(jdbcValue);
                }
                break;
            case Types.TIME:
                jdbcValue = jdbcTypeValue.getValue();
                if (jdbcValue != null) {
                    value = getTimeFormat().format(jdbcValue);
                }
                break;
            case Types.TIMESTAMP:
                jdbcValue = jdbcTypeValue.getValue();
                if (jdbcValue != null) {
                    value = getTimestampFormat().format(jdbcValue);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                jdbcValue = jdbcTypeValue.getValue(byte[].class);
                if (jdbcValue != null) {
                    value = encode((byte[]) jdbcValue);
                }
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                jdbcValue = jdbcTypeValue.getValue();
                if (jdbcValue != null) {
                    value = encode(write(jdbcValue));
                }
                break;
            case Types.CLOB:
            case Types.NCLOB:
                value = (String) jdbcTypeValue.getValue(String.class);
                break;
            case Types.REF:
                jdbcValue = jdbcTypeValue.getValue();
                if (jdbcValue != null) {
                    Ref ref = new SerialRef((Ref) jdbcValue);
                    value = encode(write(ref));
                }
                break;
            case Types.DATALINK:
                value = valueOf(jdbcTypeValue.getValue());
                break;
            case Types.BOOLEAN:
                value = valueOf(jdbcTypeValue.getValue());
                break;
            case Types.ROWID:
                jdbcValue = jdbcTypeValue.getValue();
                value = encode(((RowId) jdbcValue).getBytes());
                break;
            case Types.SQLXML:
                value = (String) jdbcTypeValue.getValue(String.class);
                break;
            default:
                throw new JdbcTypeFormatException(jdbcType, String.format("Unsupported jdbc type %s", jdbcType));
        }
        return value;
    }

    @Override
    protected void doParse(JdbcTypeValue<Object> jdbcTypeValue, String value) throws Exception {
        JdbcType<Object> jdbcType = jdbcTypeValue.getJdbcType();
        Object jdbcValue;
        switch (jdbcType.getTypeCode()) {
            case Types.BIT:
                jdbcTypeValue.setValue(!isEmpty(value) ? Boolean.parseBoolean(value) : null);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                jdbcTypeValue.setValue(!isEmpty(value) ? Short.parseShort(value) : null);
                break;
            case Types.INTEGER:
                jdbcTypeValue.setValue(!isEmpty(value) ? Integer.parseInt(value) : null);
                break;
            case Types.BIGINT:
                jdbcTypeValue.setValue(!isEmpty(value) ? Long.parseLong(value) : null);
                break;
            case Types.FLOAT:
            case Types.REAL:
                jdbcTypeValue.setValue(!isEmpty(value) ? Float.parseFloat(value) : null);
                break;
            case Types.DOUBLE:
                jdbcTypeValue.setValue(!isEmpty(value) ? Double.parseDouble(value) : null);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                jdbcTypeValue.setValue(!isEmpty(value) ? new BigDecimal(value) : null);
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                jdbcTypeValue.setValue(!isEmpty(value) ? value : null);
                break;
            case Types.DATE:
                jdbcTypeValue.setValue(!isEmpty(value) ? new Date(getDateFormat().parse(value).getTime()) : null);
                break;
            case Types.TIME:
                jdbcTypeValue.setValue(!isEmpty(value) ? new Time(getTimeFormat().parse(value).getTime()) : null);
                break;
            case Types.TIMESTAMP:
                jdbcTypeValue.setValue(
                        !isEmpty(value) ? new Timestamp(getTimestampFormat().parse(value).getTime()) : null);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                jdbcTypeValue.setValue(!isEmpty(value) ? decode(value) : null);
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                jdbcTypeValue.setValue(read(decode(value)));
                break;
            case Types.BLOB:
                jdbcTypeValue.setValue(decode(value));
                break;
            case Types.CLOB:
                jdbcTypeValue.setValue(value);
                break;
            case Types.NCLOB:
                jdbcTypeValue.setValue(value);
                break;
            case Types.REF:
                jdbcTypeValue.setValue(!isEmpty(value) ? read(decode(value)) : null);
                break;
            case Types.DATALINK:
                jdbcTypeValue.setValue(!isEmpty(value) ? new URL(value) : null);
                break;
            case Types.BOOLEAN:
                jdbcTypeValue.setValue(!isEmpty(value) ? Boolean.parseBoolean(value) : null);
                break;
            case Types.SQLXML:
                jdbcTypeValue.setValue(!isEmpty(value) ? value : null);
                break;
            default:
                throw new JdbcTypeFormatException(jdbcType, String.format("Failed parsing jdbc type %s", jdbcType));
        }
    }

    protected String encode(byte[] value) {
        return Base64.encodeBase64String(value);
    }

    protected byte[] write(Object object) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream output = null;
        try {
            output = new ObjectOutputStream(bytes);
            output.writeObject(object);
        } finally {
            closeQuietly(bytes);
            closeQuietly(output);
        }
        return bytes.toByteArray();
    }

    protected byte[] decode(String value) {
        return Base64.decodeBase64(value);
    }

    protected Object read(byte[] value) throws ClassNotFoundException, IOException {
        ByteArrayInputStream bytes = new ByteArrayInputStream(value);
        ObjectInputStream input = null;
        try {
            input = new ObjectInputStream(bytes);
            return input.readObject();
        } finally {
            closeQuietly(bytes);
            closeQuietly(input);
        }
    }

    public DateFormat getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(DateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }

    public DateFormat getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(DateFormat timeFormat) {
        this.timeFormat = timeFormat;
    }

    public DateFormat getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(DateFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
    }
}
