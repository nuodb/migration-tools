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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.url.JdbcUrl;

import java.sql.Types;

import static com.nuodb.migrator.jdbc.dialect.DialectUtils.NULL;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.url.MySQLJdbcUrl.*;
import static java.lang.String.format;

/**
 * Mimics MySQL's zeroDateTimeBehavior and converts zero'd date time values
 * following these rules:
 * <ul>
 * <li>convertToNull outputs NULL</li>
 * <li>round outputs '00:00:00' for TIME, '0001-01-01' for DATE & YEAR,
 * '0001-01-01 00:00:00' for DATETIME & TIMESTAMP types</li>
 * <li>exception causes runtime exception on the first encountered zero'd date
 * type value</li>
 * </ul>
 *
 * @author Sergey Bushik
 */
public class MySQLZeroDateTimeTranslator extends ColumnTranslatorBase {

    public static final String ZERO_TIME = "00:00:00";
    public static final String ZERO_DATE = "0000-00-00";
    public static final String ZERO_TIMESTAMP = "0000-00-00 00:00:00";
    public static final String ROUND_TIME = "00:00:00";
    public static final String ROUND_DATE = "0001-01-01";
    public static final String ROUND_TIMESTAMP = "0001-01-01 00:00:00";

    public MySQLZeroDateTimeTranslator() {
        super(MYSQL);
    }

    @Override
    protected boolean supportsScript(ColumnScript script, TranslationContext context) {
        boolean supportsScript = false;
        if (script.getScript() != null) {
            Column column = script.getColumn();
            String source = script.getScript();
            switch (column.getTypeCode()) {
            case Types.TIME:
                supportsScript = ZERO_TIME.equals(source);
                break;
            case Types.DATE:
                supportsScript = ZERO_DATE.equals(source);
                break;
            case Types.TIMESTAMP:
                supportsScript = ZERO_TIMESTAMP.equals(source);
                break;
            default:
                supportsScript = false;
            }
        }
        return supportsScript;
    }

    @Override
    public Script translate(ColumnScript script, TranslationContext context) {
        JdbcUrl jdbcUrl = getJdbcUrl(context);
        String behavior = (String) jdbcUrl.getParameters().get(ZERO_DATE_TIME_BEHAVIOR);
        if (behavior == null) {
            behavior = DEFAULT_BEHAVIOR;
        }
        String target;
        if (ROUND.equals(behavior)) {
            Column column = script.getColumn();
            switch (column.getTypeCode()) {
            case Types.TIME:
                target = ROUND_TIME;
                break;
            case Types.DATE:
                target = ROUND_DATE;
                break;
            case Types.TIMESTAMP:
                target = ROUND_TIMESTAMP;
                break;
            default:
                target = null;
            }
        } else if (!script.getColumn().isNullable() && CONVERT_TO_NULL.equals(behavior)) {
            // tagret value can't be NULL with column is 'NOT NULL'. Retain the
            // script value
            // to escape NuoDB syntax error.
            target = "'".concat(script.getScript().concat("'"));
        } else if (CONVERT_TO_NULL.equals(behavior)) {
            target = NULL;
        } else if (EXCEPTION.equals(behavior)) {
            Column column = script.getColumn();
            Table table = column.getTable();
            throw new TranslatorException(format("Table %s column %s has zero date time value, whereas %s is %s",
                    table.getQualifiedName(null), column.getName(), ZERO_DATE_TIME_BEHAVIOR, EXCEPTION));
        } else {
            target = null;
        }
        return target != null ? new SimpleScript(target, true) : null;
    }
}
