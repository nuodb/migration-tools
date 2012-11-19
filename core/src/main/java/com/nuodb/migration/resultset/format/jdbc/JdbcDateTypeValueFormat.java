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
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.apache.commons.lang.StringUtils.isEmpty;
import static com.nuodb.migration.jdbc.type.JdbcTypeDesc.isEqualTypeNames;

/**
 * @author Sergey Bushik
 */
public class JdbcDateTypeValueFormat extends JdbcTypeValueFormatBase<Date> {

    private static final String YEAR_TYPE = "YEAR";

    public static final JdbcTypeValueFormat<Date> INSTANCE = new JdbcDateTypeValueFormat();

    private static final SimpleDateFormat YEAR_FORMAT = new SimpleDateFormat("yyyy");

    @Override
    protected String doGetValue(JdbcTypeValueAccess<Date> access) throws SQLException {
        Date date = access.getValue();
        if (date == null) {
            return null;
        } else if (isEqualTypeNames(access.getColumnModel().getTypeName(), YEAR_TYPE)) {
            return YEAR_FORMAT.format(date);
        } else {
            return date.toString();
        }
    }

    @Override
    protected void doSetValue(JdbcTypeValueAccess<Date> access, String value) throws SQLException {
        if (!(doSetValueAsDate(access, value) || doSetValueAsYear(access, value))) {
            throw new JdbcTypeValueException(
                    String.format("Value %s is not date or year", value));
        }
    }

    protected boolean doSetValueAsDate(JdbcTypeValueAccess<Date> access, String value) throws SQLException {
        try {
            access.setValue(!isEmpty(value) ? Date.valueOf(value) : null);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    protected boolean doSetValueAsYear(JdbcTypeValueAccess<Date> access, String value) throws SQLException {
        try {
            access.setValue(YEAR_FORMAT.parse(value));
            return true;
        } catch (ParseException e) {
            return false;
        }
    }
}
