/**
 * Copyright (c) 2015, NuoDB, Inc.
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
package com.nuodb.migrator.backup.format.value;

import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.type.JdbcValueAccess;

import javax.sql.rowset.serial.SerialRef;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.Types;
import java.util.Map;

import static com.nuodb.migrator.backup.format.value.ValueType.BINARY;
import static com.nuodb.migrator.backup.format.value.ValueType.STRING;
import static com.nuodb.migrator.backup.format.value.ValueUtils.binary;
import static com.nuodb.migrator.backup.format.value.ValueUtils.string;
import static java.lang.String.format;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class JdbcValueFormat extends ValueFormatBase<Object> {

    @Override
    protected Value doGetValue(JdbcValueAccess<Object> access, Map<String, Object> options) throws Exception {
        Object result;
        Field field = access.getField();
        Value value;
        switch (field.getTypeCode()) {
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
            result = access.getValue(options);
            value = string(result != null ? result.toString() : null);
            break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
        case Types.NCHAR:
            value = string(access.getValue(String.class, options));
            break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BLOB:
            value = binary(access.getValue(byte[].class, options));
            break;
        case Types.OTHER:
        case Types.JAVA_OBJECT:
        case Types.STRUCT:
            result = access.getValue(options);
            value = binary(result != null ? write(result) : null);
            break;
        case Types.CLOB:
        case Types.NCLOB:
            value = string(access.getValue(String.class, options));
            break;
        case Types.REF:
            result = access.getValue(options);
            value = binary(write(new SerialRef((Ref) result)));
            break;
        case Types.DATALINK:
            result = access.getValue(options);
            value = string(result != null ? result.toString() : null);
            break;
        case Types.BOOLEAN:
            result = access.getValue(options);
            value = string(result != null ? result.toString() : null);
            break;
        case Types.ROWID:
            result = access.getValue(options);
            value = binary(result != null ? ((RowId) result).getBytes() : null);
            break;
        case Types.SQLXML:
            value = string(access.getValue(String.class, options));
            break;
        default:
            throw new ValueFormatException(format("Unsupported data type %s, type code %d on %s column",
                    field.getTypeName(), field.getTypeCode(), getColumnName(field)));
        }
        return value;
    }

    @Override
    protected void doSetValue(Value value, JdbcValueAccess<Object> access, Map<String, Object> options)
            throws Exception {
        Field field = access.getField();
        final String result = value.asString();
        switch (field.getTypeCode()) {
        case Types.BIT:
        case Types.BOOLEAN:
            access.setValue(!isEmpty(result) ? Boolean.parseBoolean(result) : null, options);
            break;
        case Types.TINYINT:
        case Types.SMALLINT:
            access.setValue(!isEmpty(result) ? Short.parseShort(result) : null, options);
            break;
        case Types.INTEGER:
            access.setValue(!isEmpty(result) ? Integer.parseInt(result) : null, options);
            break;
        case Types.BIGINT:
            access.setValue(!isEmpty(result) ? Long.parseLong(result) : null, options);
            break;
        case Types.FLOAT:
        case Types.REAL:
            access.setValue(!isEmpty(result) ? Float.parseFloat(result) : null, options);
            break;
        case Types.DOUBLE:
            access.setValue(!isEmpty(result) ? Double.parseDouble(result) : null, options);
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            access.setValue(!isEmpty(result) ? new BigDecimal(result) : null, options);
            break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
        case Types.NCHAR:
            access.setValue(result, options);
            break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            access.setValue(!isEmpty(result) ? result : null, options);
            break;
        case Types.OTHER:
        case Types.JAVA_OBJECT:
        case Types.STRUCT:
            access.setValue(read(value.asBytes()), options);
            break;
        case Types.BLOB:
            access.setValue(value.asBytes(), options);
            break;
        case Types.CLOB:
            access.setValue(result, options);
            break;
        case Types.NCLOB:
            access.setValue(result, options);
            break;
        case Types.REF:
            access.setValue(!isEmpty(result) ? read(value.asBytes()) : null, options);
            break;
        case Types.DATALINK:
            access.setValue(!isEmpty(result) ? new URL(result) : null, options);
            break;
        case Types.SQLXML:
            access.setValue(!isEmpty(result) ? result : null, options);
            break;
        default:
            throw new ValueFormatException(format("Unsupported data type %s, type code %d on %s column",
                    field.getTypeName(), field.getTypeCode(), getColumnName(field)));
        }
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

    @Override
    public ValueType getValueType(Field field) {
        ValueType valueType;
        switch (field.getTypeCode()) {
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BLOB:
        case Types.OTHER:
        case Types.JAVA_OBJECT:
        case Types.STRUCT:
        case Types.REF:
        case Types.DATALINK:
        case Types.ROWID:
            valueType = BINARY;
            break;
        default:
            valueType = STRING;
            break;
        }
        return valueType;
    }
}