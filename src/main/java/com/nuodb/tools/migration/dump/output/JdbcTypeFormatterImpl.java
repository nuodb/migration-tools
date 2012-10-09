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
import com.nuodb.tools.migration.jdbc.type.JdbcTypeUtils;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.*;
import java.text.Format;
import java.text.SimpleDateFormat;

import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeFormatterImpl implements JdbcTypeFormatter {

    public static final Format DATE_FORMAT = new SimpleDateFormat("dd-MMM-yyyy");

    public static final Format TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

    public static final Format TIMESTAMP_FORMAT = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

    private Format dateFormat = DATE_FORMAT;
    private Format timeFormat = TIME_FORMAT;
    private Format timestampFormat = TIMESTAMP_FORMAT;

    public String format(Object value, int type) {
        String result;
        if (value != null) {
            switch (type) {
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
                    result = valueOf(value);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    result = valueOf(value);
                    break;
                case Types.DATE:
                    result = getDateFormat().format(value);
                    break;
                case Types.TIME:
                    result = getTimeFormat().toString();
                    break;
                case Types.TIMESTAMP:
                    result = getTimestampFormat().format(value);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    result = encode((byte[]) value);
                    break;
                case Types.OTHER:
                case Types.JAVA_OBJECT:
                case Types.STRUCT:
                    try {
                        result = encode(serialize(value));
                    } catch (IOException e) {
                        throw new OutputFormatException("Failed serializing jdbc object", e);
                    }
                    break;
                case Types.ARRAY:
                    try {
                        Object array = ((Array) value).getArray();
                        result = encode(serialize(array));
                    } catch (IOException e) {
                        throw new OutputFormatException("Failed serializing jdbc array", e);
                    } catch (SQLException e) {
                        throw new OutputFormatException("Failed processing jdbc array", e);
                    }
                    break;
                case Types.BLOB:
                    try {
                        result = encode(JdbcTypeUtils.read((Blob) value));
                    } catch (IOException e) {
                        throw new OutputFormatException("Failed reading jdbc blob", e);
                    } catch (SQLException e) {
                        throw new OutputFormatException("Failed processing jdbc blob", e);
                    }
                    break;
                case Types.CLOB:
                case Types.NCHAR:
                    try {
                        result = JdbcTypeUtils.read((Clob) value);
                    } catch (IOException e) {
                        throw new OutputFormatException("Failed reading jdbc clob", e);
                    } catch (SQLException e) {
                        throw new OutputFormatException("Failed processing jdbc clob", e);
                    }
                    break;
                case Types.REF:
                    try {
                        result = encode(serialize(((Ref)value).getObject()));
                    } catch (IOException e) {
                        throw new OutputFormatException("Failed reading jdbc ref", e);
                    } catch (SQLException e) {
                        throw new OutputFormatException("Failed processing jdbc ref", e);
                    }
                    break;
                case Types.DATALINK:
                    result = valueOf(value);
                    break;
                case Types.BOOLEAN:
                    result = valueOf(value);
                    break;
                case Types.ROWID:
                    result = encode(((RowId)value).getBytes());
                    break;
                case Types.SQLXML:
                    try {
                        result = ((SQLXML)value).getString();
                    } catch (SQLException e) {
                        throw new OutputFormatException("Failed reading jdbc xml", e);
                    }
                    break;
                default:
                    throw new OutputFormatException(String.format("Jdbc type %1$d formatting unsupported", type));
            }
        } else {
            result = "";
        }
        return result;
    }

    protected byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream output = null;
        try {
            output = new ObjectOutputStream(bytes);
            output.writeObject(object);
        } finally {
            if (output != null) {
                output.close();
            }
        }
        return bytes.toByteArray();
    }

    protected String encode(byte[] value) {
        return Base64.encodeBase64String(value);
    }

    public Format getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(Format dateFormat) {
        this.dateFormat = dateFormat;
    }

    public Format getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(Format timeFormat) {
        this.timeFormat = timeFormat;
    }

    public Format getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(Format timestampFormat) {
        this.timestampFormat = timestampFormat;
    }
}
