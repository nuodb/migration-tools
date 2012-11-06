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
package com.nuodb.migration.result.format.jdbc;

import org.apache.commons.codec.binary.Base64;

import javax.sql.rowset.serial.SerialRef;
import java.io.*;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Types;

import static com.nuodb.migration.jdbc.type.JdbcTypeCodeNameMap.INSTANCE;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcTypeValueFormatImpl implements JdbcTypeValueFormat<Object> {

    @Override
    public String getValue(JdbcTypeValueAccessor accessor) {
        final int typeCode = accessor.getValueModel().getTypeCode();
        Object jdbcValue;
        String value = null;
        switch (typeCode) {
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
                    jdbcValue = accessor.getValue();
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                try {
                    jdbcValue = accessor.getValue();
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                try {
                    jdbcValue = accessor.getValue(Long.class);
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                try {
                    jdbcValue = accessor.getValue(byte[].class);
                    if (jdbcValue != null) {
                        value = encode((byte[]) jdbcValue);
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                try {
                    jdbcValue = accessor.getValue();
                    if (jdbcValue != null) {
                        value = encode(write(jdbcValue));
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                } catch (IOException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.CLOB:
            case Types.NCLOB:
                try {
                    value = (String) accessor.getValue(String.class);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.REF:
                try {
                    jdbcValue = accessor.getValue();
                    if (jdbcValue != null) {
                        Ref ref = new SerialRef((Ref) jdbcValue);
                        value = encode(write(ref));
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                } catch (IOException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.DATALINK:
                try {
                    jdbcValue = accessor.getValue();
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.BOOLEAN:
                try {
                    jdbcValue = accessor.getValue();
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.ROWID:
                try {
                    jdbcValue = accessor.getValue();
                    if (jdbcValue != null) {
                        value = encode(((RowId) jdbcValue).getBytes());
                    }
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.SQLXML:
                try {
                    value = (String) accessor.getValue(String.class);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            default:
                throw new JdbcTypeValueException(
                        String.format("Unsupported jdbc type %s", INSTANCE.getTypeName(typeCode)));
        }
        return value;
    }

    @Override
    public void setValue(JdbcTypeValueAccessor<Object> accessor, String value) {
        final int typeCode = accessor.getValueModel().getTypeCode();
        switch (typeCode) {
            case Types.BIT:
            case Types.BOOLEAN:
                try {
                    accessor.setValue(!isEmpty(value) ? Boolean.parseBoolean(value) : null);
                    accessor.setValue(!isEmpty(value) ? Boolean.parseBoolean(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                try {
                    accessor.setValue(!isEmpty(value) ? Short.parseShort(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.INTEGER:
                try {
                    accessor.setValue(!isEmpty(value) ? Integer.parseInt(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.BIGINT:
                try {
                    accessor.setValue(!isEmpty(value) ? Long.parseLong(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.FLOAT:
            case Types.REAL:
                try {
                    accessor.setValue(!isEmpty(value) ? Float.parseFloat(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.DOUBLE:
                try {
                    accessor.setValue(!isEmpty(value) ? Double.parseDouble(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                try {
                    accessor.setValue(!isEmpty(value) ? new BigDecimal(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                try {
                    accessor.setValue(!isEmpty(value) ? value : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                try {
                    accessor.setValue(!isEmpty(value) ? Long.parseLong(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                try {
                    accessor.setValue(!isEmpty(value) ? decode(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                try {
                    accessor.setValue(read(decode(value)));
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                } catch (ClassNotFoundException exception) {
                    throw new JdbcTypeValueException(exception);
                } catch (IOException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.BLOB:
                try {
                    accessor.setValue(decode(value));
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.CLOB:
                try {
                    accessor.setValue(value);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.NCLOB:
                try {
                    accessor.setValue(value);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.REF:
                try {
                    accessor.setValue(!isEmpty(value) ? read(decode(value)) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                } catch (ClassNotFoundException exception) {
                    throw new JdbcTypeValueException(exception);
                } catch (IOException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.DATALINK:
                try {
                    accessor.setValue(!isEmpty(value) ? new URL(value) : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                } catch (MalformedURLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            case Types.SQLXML:
                try {
                    accessor.setValue(!isEmpty(value) ? value : null);
                } catch (SQLException exception) {
                    throw new JdbcTypeValueException(exception);
                }
                break;
            default:
                throw new JdbcTypeValueException(
                        String.format("Unsupported jdbc type %s", INSTANCE.getTypeName(typeCode)));
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
}
