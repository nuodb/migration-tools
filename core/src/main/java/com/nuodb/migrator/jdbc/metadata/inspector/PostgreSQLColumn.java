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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.AutoIncrement;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.DefaultValue;
import com.nuodb.migrator.jdbc.metadata.Sequence;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.nuodb.migrator.jdbc.dialect.DialectUtils.stripQuotes;
import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLColumn {

    private static final String VALUE_CLASS_REGEX = "'(.*)'::.*";
    private static final String AUTO_INCREMENT_REGEX = "nextval\\(" + VALUE_CLASS_REGEX + "\\)";
    private static final Pattern VALUE_CLASS = compile(VALUE_CLASS_REGEX, CASE_INSENSITIVE);
    private static final Pattern AUTO_INCREMENT = compile(AUTO_INCREMENT_REGEX, CASE_INSENSITIVE);

    public static Column process(InspectionContext inspectionContext, Column column) throws SQLException {
        DefaultValue defaultValue = column.getDefaultValue();
        if (defaultValue != null) {
            Matcher matcher;
            if ((matcher = AUTO_INCREMENT.matcher(defaultValue.getValue())).matches()) {
                Sequence sequence = new AutoIncrement();
                sequence.setName(stripQuotes(inspectionContext.getDialect(), matcher.group(1)));
                column.setAutoIncrement(true);
                column.setDefaultValue(null);
                column.setSequence(sequence);
            } else if ((matcher = VALUE_CLASS.matcher(defaultValue.getValue())).matches()) {
                column.setDefaultValue(valueOf(matcher.group(1)));
            }
        }
        return column;
    }
}
