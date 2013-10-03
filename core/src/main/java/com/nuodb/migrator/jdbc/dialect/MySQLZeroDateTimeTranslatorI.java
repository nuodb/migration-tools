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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.connection.ConnectionProxy;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;
import com.nuodb.migrator.jdbc.url.JdbcUrl;
import com.nuodb.migrator.spec.DriverConnectionSpec;

import java.sql.Types;

import static com.nuodb.migrator.jdbc.resolve.DatabaseInfoUtils.MYSQL;
import static com.nuodb.migrator.jdbc.url.MySQLJdbcUrlParser.*;
import static java.lang.String.format;

/**
 * Mimics MySQL's zeroDateTimeBehavior and converts zero'd date time values following these rules:
 * <ul>
 *     <li>convertToNull outputs NULL</li>
 *     <li>round outputs '00:00:00' for TIME, '0001-01-01' for DATE & YEAR,
 *     '0001-01-01 00:00:00' for DATETIME & TIMESTAMP types</li>
 *     <li>exception causes runtime exception on the first encountered zero'd date type value</li>
 * </ul>
 *
 * @author Sergey Bushik
 */
public class MySQLZeroDateTimeTranslatorI extends ColumnTranslatorBase {

    private static final String ZERO_TIME = "00:00:00";
    private static final String ZERO_DATE = "0000-00-00";
    private static final String ZERO_TIMESTAMP = "0000-00-00 00:00:00";
    private static final String ROUND_TIME = "00:00:00";
    private static final String ROUND_DATE = "0001-01-01";
    private static final String ROUND_TIMESTAMP = "0001-01-01 00:00:00";
    private static final String NULL = "NULL";

    public MySQLZeroDateTimeTranslatorI() {
        super(MYSQL);
    }

    protected boolean canTranslate(Script script, Column column, DatabaseInfo databaseInfo) {
        String literal = column.getDefaultValue().getScript();
        boolean canTranslate;
        switch (column.getTypeCode()) {
            case Types.TIME:
                canTranslate = ZERO_TIME.equals(literal);
                break;
            case Types.DATE:
                canTranslate = ZERO_DATE.equals(literal);
                break;
            case Types.TIMESTAMP:
                canTranslate = ZERO_TIMESTAMP.equals(literal);
                break;
            default:
                canTranslate = false;
        }
        return canTranslate;
    }

    @Override
    protected Script translate(Script script, Column column, DatabaseInfo databaseInfo) {
        JdbcUrl jdbcUrl = getJdbcUrl(script);
        String behavior = (String) jdbcUrl.getParameters().get(ZERO_DATE_TIME_BEHAVIOR);
        if (behavior == null) {
            behavior = DEFAULT_BEHAVIOR;
        }
        String translation;
        if (ROUND.equals(behavior)) {
            switch (column.getTypeCode()) {
                case Types.TIME:
                    translation = ROUND_TIME;
                    break;
                case Types.DATE:
                    translation = ROUND_DATE;
                    break;
                case Types.TIMESTAMP:
                    translation = ROUND_TIMESTAMP;
                    break;
                default:
                    translation = null;
            }
        } else if (CONVERT_TO_NULL.equals(behavior)) {
            translation = NULL;
        } else if (EXCEPTION.equals(behavior)) {
            throw new TranslatorException(
                    format("Encountered zero date time value, whereas %s is %s", ZERO_DATE_TIME_BEHAVIOR, EXCEPTION));
        } else {
            translation = null;
        }
        return translation != null ? new SimpleScript(translation, databaseInfo) : null;
    }

    protected JdbcUrl getJdbcUrl(Script script) {
        ConnectionProxy connection = (ConnectionProxy) script.getSession().getConnection();
        return ((DriverConnectionSpec)connection.getConnectionSpec()).getJdbcUrl();
    }
}
