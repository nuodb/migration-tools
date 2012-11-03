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
package com.nuodb.tools.migration.result.format;

import com.nuodb.tools.migration.jdbc.type.JdbcType;
import org.apache.commons.codec.binary.Base64;

import javax.sql.rowset.serial.SerialRef;
import java.io.*;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcTypeFormatImpl implements JdbcTypeFormat<Object> {

    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public static final DateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

    public static final DateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private DateFormat dateFormat = DATE_FORMAT;
    private DateFormat timeFormat = TIME_FORMAT;
    private DateFormat timestampFormat = TIMESTAMP_FORMAT;

    @Override
    public String getValue(JdbcTypeValue jdbcTypeValue) {
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
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.DATE:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = getDateFormat().format(jdbcValue);
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.TIME:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = getTimeFormat().format(jdbcValue);
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.TIMESTAMP:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = getTimestampFormat().format(jdbcValue);
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                try {
                    jdbcValue = jdbcTypeValue.getValue(byte[].class);
                    if (jdbcValue != null) {
                        value = encode((byte[]) jdbcValue);
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = encode(write(jdbcValue));
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (IOException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.CLOB:
            case Types.NCLOB:
                try {
                    value = (String) jdbcTypeValue.getValue(String.class);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.REF:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        Ref ref = new SerialRef((Ref) jdbcValue);
                        value = encode(write(ref));
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (IOException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.DATALINK:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.BOOLEAN:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.ROWID:
                try {
                    jdbcValue = jdbcTypeValue.getValue();
                    if (jdbcValue != null) {
                        value = encode(((RowId) jdbcValue).getBytes());
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.SQLXML:
                try {
                    value = (String) jdbcTypeValue.getValue(String.class);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            default:
                throw new JdbcTypeFormatException(String.format("Unsupported jdbc type %s", jdbcType));
        }
        return value;
    }

    @Override
    public void setValue(JdbcTypeValue<Object> jdbcTypeValue, String value) {
        JdbcType<Object> jdbcType = jdbcTypeValue.getJdbcType();
        switch (jdbcType.getTypeCode()) {
            case Types.BIT:
            case Types.BOOLEAN:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? Boolean.parseBoolean(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? Short.parseShort(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.INTEGER:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? Integer.parseInt(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.BIGINT:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? Long.parseLong(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.FLOAT:
            case Types.REAL:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? Float.parseFloat(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.DOUBLE:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? Double.parseDouble(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? new BigDecimal(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? value : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.DATE:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? new Date(getDateFormat().parse(value).getTime()) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (ParseException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.TIME:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? new Time(getTimeFormat().parse(value).getTime()) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (ParseException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.TIMESTAMP:
                try {
                    jdbcTypeValue.setValue(
                            !isEmpty(value) ? new Timestamp(getTimestampFormat().parse(value).getTime()) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (ParseException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? decode(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                try {
                    jdbcTypeValue.setValue(read(decode(value)));
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (ClassNotFoundException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (IOException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.BLOB:
                try {
                    jdbcTypeValue.setValue(decode(value));
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.CLOB:
                try {
                    jdbcTypeValue.setValue(value);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.NCLOB:
                try {
                    jdbcTypeValue.setValue(value);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.REF:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? read(decode(value)) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (ClassNotFoundException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (IOException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.DATALINK:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? new URL(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                } catch (MalformedURLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            case Types.SQLXML:
                try {
                    jdbcTypeValue.setValue(!isEmpty(value) ? value : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeFormatException(exception);
                }
                break;
            default:
                throw new JdbcTypeFormatException(String.format("Failed parsing jdbc type %s", jdbcType));
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
