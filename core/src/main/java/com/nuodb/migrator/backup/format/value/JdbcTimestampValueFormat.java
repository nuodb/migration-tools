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

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import static com.nuodb.migrator.backup.format.value.ValueUtils.string;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class JdbcTimestampValueFormat extends ValueFormatBase<Timestamp> {

    private static final DateFormat YEAR_FORMAT = new SimpleDateFormat("yyyy");

    @Override
    protected Value doGetValue(JdbcValueAccess<Timestamp> access, Map<String, Object> options) throws SQLException {
        Timestamp timestamp = access.getValue(options);
        return string(timestamp != null ? timestamp.toString() : null);
    }

    @Override
    protected void doSetValue(Value value, JdbcValueAccess<Timestamp> access, Map<String, Object> options)
            throws SQLException {
        if (!(doSetValueAsTimestamp(value, access, options) || doSetValueAsDate(value, access, options)
                || doSetValueAsYear(value, access, options))) {
            throw new ValueFormatException(format("Value %s is not a timestamp, date nor year", value));
        }
    }

    protected boolean doSetValueAsTimestamp(Value variant, JdbcValueAccess<Timestamp> access,
            Map<String, Object> options) throws SQLException {
        try {
            String value = variant.asString();
            access.setValue(!isEmpty(value) ? Timestamp.valueOf(value) : null, options);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    protected boolean doSetValueAsDate(Value variant, JdbcValueAccess<Timestamp> access, Map<String, Object> options)
            throws SQLException {
        try {
            String value = variant.asString();
            access.setValue(!isEmpty(value) ? Date.valueOf(value) : null, options);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    protected boolean doSetValueAsYear(Value variant, JdbcValueAccess<Timestamp> access, Map<String, Object> options)
            throws SQLException {
        try {
            String value = variant.asString();
            access.setValue(YEAR_FORMAT.parse(value), options);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    @Override
    public ValueType getValueType(Field field) {
        return ValueType.STRING;
    }
}