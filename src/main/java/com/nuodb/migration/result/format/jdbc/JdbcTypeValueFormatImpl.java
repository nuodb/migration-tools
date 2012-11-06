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

import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessor;
import org.apache.commons.codec.binary.Base64;

import javax.sql.rowset.serial.SerialRef;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.Types;

import static com.nuodb.migration.jdbc.type.JdbcTypeNameMap.INSTANCE;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcTypeValueFormatImpl extends JdbcTypeValueFormatBase<Object> {

    @Override
    protected String doGetValue(JdbcTypeValueAccessor<Object> accessor) throws Exception {
        Object jdbcValue;
        String value = null;
        switch (accessor.getValueModel().getTypeCode()) {
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
                jdbcValue = accessor.getValue();
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
                value = accessor.getValue(String.class);
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                jdbcValue = accessor.getValue(Long.class);
                if (jdbcValue != null) {
                    value = jdbcValue.toString();
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                jdbcValue = accessor.getValue(byte[].class);
                if (jdbcValue != null) {
                    value = encode((byte[]) jdbcValue);
                }
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                jdbcValue = accessor.getValue();
                if (jdbcValue != null) {
                    value = encode(write(jdbcValue));
                }
                break;
            case Types.CLOB:
            case Types.NCLOB:
                value = accessor.getValue(String.class);
                break;
            case Types.REF:
                jdbcValue = accessor.getValue();
                if (jdbcValue != null) {
                    Ref ref = new SerialRef((Ref) jdbcValue);
                    value = encode(write(ref));
                }
                break;
            case Types.DATALINK:
                jdbcValue = accessor.getValue();
                if (jdbcValue != null) {
                    value = jdbcValue.toString();
                }
                break;
            case Types.BOOLEAN:
                jdbcValue = accessor.getValue();
                if (jdbcValue != null) {
                    value = jdbcValue.toString();
                }
                break;
            case Types.ROWID:
                jdbcValue = accessor.getValue();
                if (jdbcValue != null) {
                    value = encode(((RowId) jdbcValue).getBytes());
                }
                break;
            case Types.SQLXML:
                value = accessor.getValue(String.class);
                break;
            default:
                throw new JdbcTypeValueException(
                        String.format("Unsupported jdbc type %s",
                                INSTANCE.getTypeName(accessor.getValueModel().getTypeCode())));
        }
        return value;
    }

    @Override
    protected void doSetValue(JdbcTypeValueAccessor<Object> accessor, String value) throws Exception {
        switch (accessor.getValueModel().getTypeCode()) {
            case Types.BIT:
            case Types.BOOLEAN:
                accessor.setValue(!isEmpty(value) ? Boolean.parseBoolean(value) : null);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                accessor.setValue(!isEmpty(value) ? Short.parseShort(value) : null);
                break;
            case Types.INTEGER:
                accessor.setValue(!isEmpty(value) ? Integer.parseInt(value) : null);
                break;
            case Types.BIGINT:
                accessor.setValue(!isEmpty(value) ? Long.parseLong(value) : null);
                break;
            case Types.FLOAT:
            case Types.REAL:
                accessor.setValue(!isEmpty(value) ? Float.parseFloat(value) : null);
                break;
            case Types.DOUBLE:
                accessor.setValue(!isEmpty(value) ? Double.parseDouble(value) : null);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                accessor.setValue(!isEmpty(value) ? new BigDecimal(value) : null);
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                accessor.setValue(!isEmpty(value) ? value : null);
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                accessor.setValue(!isEmpty(value) ? Long.parseLong(value) : null);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                accessor.setValue(!isEmpty(value) ? decode(value) : null);
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                accessor.setValue(read(decode(value)));
                break;
            case Types.BLOB:
                accessor.setValue(decode(value));
                break;
            case Types.CLOB:
                accessor.setValue(value);
                break;
            case Types.NCLOB:
                accessor.setValue(value);
                break;
            case Types.REF:
                accessor.setValue(!isEmpty(value) ? read(decode(value)) : null);
                break;
            case Types.DATALINK:
                accessor.setValue(!isEmpty(value) ? new URL(value) : null);
                break;
            case Types.SQLXML:
                accessor.setValue(!isEmpty(value) ? value : null);
                break;
            default:
                throw new JdbcTypeValueException(
                        String.format("Unsupported jdbc type %s",
                                INSTANCE.getTypeName(accessor.getValueModel().getTypeCode())));
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
