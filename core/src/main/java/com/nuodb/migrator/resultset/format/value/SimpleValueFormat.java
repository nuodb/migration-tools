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
package com.nuodb.migrator.resultset.format.value;

import com.nuodb.migrator.jdbc.model.ValueModel;
import com.nuodb.migrator.jdbc.type.access.JdbcTypeValueAccess;

import javax.sql.rowset.serial.SerialRef;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.Types;
import java.util.Map;

import static com.nuodb.migrator.resultset.format.value.ValueVariantType.BYTES;
import static com.nuodb.migrator.resultset.format.value.ValueVariantType.STRING;
import static com.nuodb.migrator.resultset.format.value.ValueVariants.binary;
import static com.nuodb.migrator.resultset.format.value.ValueVariants.string;
import static java.lang.String.format;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class SimpleValueFormat extends ValueFormatBase<Object> {

    @Override
    protected ValueVariant doGetValue(JdbcTypeValueAccess<Object> access,
                                 Map<String, Object> accessOptions) throws Exception {
        Object value;
        ValueModel valueModel = access.getValueModel();
        ValueVariant variant;
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
                value = access.getValue(accessOptions);
                variant = string(value != null ? value.toString() : null);
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                variant = string(access.getValue(String.class, accessOptions));
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                variant = binary(access.getValue(byte[].class, accessOptions));
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                value = access.getValue(accessOptions);
                variant = binary(value != null ? write(value) : null);
                break;
            case Types.CLOB:
            case Types.NCLOB:
                variant = string(access.getValue(String.class, accessOptions));
                break;
            case Types.REF:
                value = access.getValue(accessOptions);
                variant = binary(write(new SerialRef((Ref) value)));
                break;
            case Types.DATALINK:
                value = access.getValue(accessOptions);
                variant = string(value != null ? value.toString() : null);
                break;
            case Types.BOOLEAN:
                value = access.getValue(accessOptions);
                variant = string(value != null ? value.toString() : null);
                break;
            case Types.ROWID:
                value = access.getValue(accessOptions);
                variant = binary(value != null ? ((RowId) value).getBytes() : null);
                break;
            case Types.SQLXML:
                variant = string(access.getValue(String.class, accessOptions));
                break;
            default:
                throw new ValueFormatException(format("Unsupported data type %s, type code %d on %s column",
                        valueModel.getTypeName(), valueModel.getTypeCode(), getValueName(valueModel)));
        }
        return variant;
    }

    @Override
    protected void doSetValue(ValueVariant variant, JdbcTypeValueAccess<Object> access,
                              Map<String, Object> accessOptions) throws Exception {
        ValueModel valueModel = access.getValueModel();
        final String value = variant.asString();
        switch (valueModel.getTypeCode()) {
            case Types.BIT:
            case Types.BOOLEAN:
                access.setValue(!isEmpty(value) ? Boolean.parseBoolean(value) : null, accessOptions);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                if (access.getValueModel().getScale() > 0) {
                }
                access.setValue(!isEmpty(value) ? Short.parseShort(value) : null, accessOptions);
                break;
            case Types.INTEGER:
                access.setValue(!isEmpty(value) ? Integer.parseInt(value) : null, accessOptions);
                break;
            case Types.BIGINT:
                access.setValue(!isEmpty(value) ? Long.parseLong(value) : null, accessOptions);
                break;
            case Types.FLOAT:
            case Types.REAL:
                access.setValue(!isEmpty(value) ? Float.parseFloat(value) : null, accessOptions);
                break;
            case Types.DOUBLE:
                access.setValue(!isEmpty(value) ? Double.parseDouble(value) : null, accessOptions);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                access.setValue(!isEmpty(value) ? new BigDecimal(value) : null, accessOptions);
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCHAR:
                access.setValue(value, accessOptions);
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                access.setValue(!isEmpty(value) ? value : null, accessOptions);
                break;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.STRUCT:
                access.setValue(read(variant.asBytes()), accessOptions);
                break;
            case Types.BLOB:
                access.setValue(variant.asBytes(), accessOptions);
                break;
            case Types.CLOB:
                access.setValue(value, accessOptions);
                break;
            case Types.NCLOB:
                access.setValue(value, accessOptions);
                break;
            case Types.REF:
                access.setValue(!isEmpty(value) ? read(variant.asBytes()) : null, accessOptions);
                break;
            case Types.DATALINK:
                access.setValue(!isEmpty(value) ? new URL(value) : null, accessOptions);
                break;
            case Types.SQLXML:
                access.setValue(!isEmpty(value) ? value : null, accessOptions);
                break;
            default:
                throw new ValueFormatException(format("Unsupported data type %s, type code %d on %s column",
                        valueModel.getTypeName(), valueModel.getTypeCode(), getValueName(valueModel)));
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
    public ValueVariantType getValueType(ValueModel valueModel) {
        ValueVariantType valueVariantType;
        switch (valueModel.getTypeCode()) {
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
                valueVariantType = BYTES;
                break;
            default:
                valueVariantType = STRING;
                break;
        }
        return valueVariantType;
    }
}