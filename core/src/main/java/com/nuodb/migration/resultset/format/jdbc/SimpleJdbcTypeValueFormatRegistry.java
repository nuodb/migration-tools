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
package com.nuodb.migration.resultset.format.jdbc;

import com.nuodb.migration.jdbc.model.ValueModel;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccess;
import com.nuodb.migration.jdbc.type.jdbc2.JdbcDateType;
import com.nuodb.migration.jdbc.type.jdbc2.JdbcTimeType;
import com.nuodb.migration.jdbc.type.jdbc2.JdbcTimestampType;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import javax.sql.rowset.serial.SerialRef;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.Types;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class SimpleJdbcTypeValueFormatRegistry extends JdbcTypeValueFormatRegistryBase {

    public SimpleJdbcTypeValueFormatRegistry() {
        super(new SimpleJdbcTypeValueFormat());
        addJdbcTypeValueFormat(JdbcTimestampType.INSTANCE, new JdbcTimestampTypeValueFormat());
        addJdbcTypeValueFormat(JdbcTimeType.INSTANCE, new JdbcTimeTypeValueFormat());
        addJdbcTypeValueFormat(JdbcDateType.INSTANCE, new JdbcDateTypeValueFormat());
    }

    static class SimpleJdbcTypeValueFormat extends JdbcTypeValueFormatBase<Object> {

        @Override
        protected String doGetValue(JdbcTypeValueAccess<Object> access, Map<String, Object> options) throws Exception {
            Object jdbcValue;
            String value = null;
            ValueModel valueModel = access.getValueModel();
            switch (valueModel.getTypeCode()) {
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
                    jdbcValue = access.getValue(options);
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.NCHAR:
                    value = access.getValue(String.class, options);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    jdbcValue = access.getValue(byte[].class, options);
                    if (jdbcValue != null) {
                        value = encode((byte[]) jdbcValue);
                    }
                    break;
                case Types.OTHER:
                case Types.JAVA_OBJECT:
                case Types.STRUCT:
                    jdbcValue = access.getValue(options);
                    if (jdbcValue != null) {
                        value = encode(write(jdbcValue));
                    }
                    break;
                case Types.CLOB:
                case Types.NCLOB:
                    value = access.getValue(String.class, options);
                    break;
                case Types.REF:
                    jdbcValue = access.getValue(options);
                    if (jdbcValue != null) {
                        Ref ref = new SerialRef((Ref) jdbcValue);
                        value = encode(write(ref));
                    }
                    break;
                case Types.DATALINK:
                    jdbcValue = access.getValue(options);
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                    break;
                case Types.BOOLEAN:
                    jdbcValue = access.getValue(options);
                    if (jdbcValue != null) {
                        value = jdbcValue.toString();
                    }
                    break;
                case Types.ROWID:
                    jdbcValue = access.getValue(options);
                    if (jdbcValue != null) {
                        value = encode(((RowId) jdbcValue).getBytes());
                    }
                    break;
                case Types.SQLXML:
                    value = access.getValue(String.class, options);
                    break;
                default:
                    throw new JdbcTypeValueException(format("Unsupported data type %s, type code %d on %s column",
                            valueModel.getTypeName(), valueModel.getTypeCode(), getValueModelName(valueModel)));
            }
            return value;
        }

        @Override
        protected void doSetValue(JdbcTypeValueAccess<Object> access, String value,
                                  Map<String, Object> options) throws Exception {
            ValueModel valueModel = access.getValueModel();
            switch (valueModel.getTypeCode()) {
                case Types.BIT:
                case Types.BOOLEAN:
                    access.setValue(!isEmpty(value) ? Boolean.parseBoolean(value) : null, options);
                    break;
                case Types.TINYINT:
                case Types.SMALLINT:
                    access.setValue(!isEmpty(value) ? Short.parseShort(value) : null, options);
                    break;
                case Types.INTEGER:
                    access.setValue(!isEmpty(value) ? Integer.parseInt(value) : null, options);
                    break;
                case Types.BIGINT:
                    access.setValue(!isEmpty(value) ? Long.parseLong(value) : null, options);
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    access.setValue(!isEmpty(value) ? Float.parseFloat(value) : null, options);
                    break;
                case Types.DOUBLE:
                    access.setValue(!isEmpty(value) ? Double.parseDouble(value) : null, options);
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    access.setValue(!isEmpty(value) ? new BigDecimal(value) : null, options);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.NCHAR:
                    access.setValue(value, options);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    access.setValue(!isEmpty(value) ? decode(value) : null, options);
                    break;
                case Types.OTHER:
                case Types.JAVA_OBJECT:
                case Types.STRUCT:
                    access.setValue(read(decode(value)), options);
                    break;
                case Types.BLOB:
                    access.setValue(decode(value), options);
                    break;
                case Types.CLOB:
                    access.setValue(value, options);
                    break;
                case Types.NCLOB:
                    access.setValue(value, options);
                    break;
                case Types.REF:
                    access.setValue(!isEmpty(value) ? read(decode(value)) : null, options);
                    break;
                case Types.DATALINK:
                    access.setValue(!isEmpty(value) ? new URL(value) : null, options);
                    break;
                case Types.SQLXML:
                    access.setValue(!isEmpty(value) ? value : null, options);
                    break;
                default:
                    throw new JdbcTypeValueException(format("Unsupported data type %s, type code %d on %s column",
                            valueModel.getTypeName(), valueModel.getTypeCode(), getValueModelName(valueModel)));
            }
        }

        protected String encode(byte[] value) {
            return value != null ? new String(Hex.encodeHex(value, false)) : null;
        }

        protected byte[] write(Object object) throws IOException {
            if (object == null) {
                return null;
            }
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
            try {
                return value != null ? Hex.decodeHex(value.toCharArray()) : null;
            } catch (DecoderException exception) {
                throw new JdbcTypeValueException(exception);
            }
        }

        protected Object read(byte[] value) throws ClassNotFoundException, IOException {
            if (value == null) {
                return null;
            }
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
}


