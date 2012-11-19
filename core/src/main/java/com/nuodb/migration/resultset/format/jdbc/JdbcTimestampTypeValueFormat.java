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

import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccess;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class JdbcTimestampTypeValueFormat extends JdbcTypeValueFormatBase<Timestamp> {

    public static final JdbcTypeValueFormat<Timestamp> INSTANCE = new JdbcTimestampTypeValueFormat();

    private static final DateFormat YEAR_FORMAT = new SimpleDateFormat("yyyy");

    @Override
    protected String doGetValue(JdbcTypeValueAccess<Timestamp> access) throws SQLException {
        Timestamp timestamp = access.getValue();
        return timestamp != null ? timestamp.toString() : null;
    }

    @Override
    protected void doSetValue(JdbcTypeValueAccess<Timestamp> access, String value) throws SQLException {
        if (!(doSetValueAsTimestamp(access, value) || doSetValueAsDate(access, value) || doSetValueAsYear(access,
                value))) {
            throw new JdbcTypeValueException(format("Value %s is not timestamp, date or year", value));
        }
    }

    protected boolean doSetValueAsTimestamp(JdbcTypeValueAccess<Timestamp> access, String value) throws SQLException {
        try {
            access.setValue(!isEmpty(value) ? Timestamp.valueOf(value) : null);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    protected boolean doSetValueAsDate(JdbcTypeValueAccess<Timestamp> access, String value) throws SQLException {
        try {
            access.setValue(!isEmpty(value) ? Date.valueOf(value) : null);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    protected boolean doSetValueAsYear(JdbcTypeValueAccess<Timestamp> access, String value) throws SQLException {
        try {
            access.setValue(YEAR_FORMAT.parse(value));
            return true;
        } catch (ParseException e) {
            return false;
        }
    }
}
