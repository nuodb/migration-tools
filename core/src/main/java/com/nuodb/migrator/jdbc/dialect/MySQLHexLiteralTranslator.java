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

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.nuodb.migrator.jdbc.resolve.DatabaseInfoUtils.MYSQL;
import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import static java.sql.Types.*;

/**
 * @author Sergey Bushik
 */
public class MySQLHexLiteralTranslator extends ColumnTranslatorBase {

    private Collection<Integer> HEX_TYPES = Arrays.asList(
            TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE, NUMERIC, DECIMAL);
    private static final Pattern PATTERN = Pattern.compile("(?i:x)'([0-9a-f]*)'");

    public MySQLHexLiteralTranslator() {
        super(MYSQL);
    }

    @Override
    protected boolean canTranslate(Script script, Column column, DatabaseInfo databaseInfo) {
        return HEX_TYPES.contains(column.getTypeCode()) && PATTERN.matcher(script.getScript()).matches();
    }

    @Override
    protected Script translate(Script script, Column column, DatabaseInfo databaseInfo) {
        Matcher matcher = PATTERN.matcher(script.getScript());
        Integer target;
        if (matcher.matches()) {
            String literal = matcher.group(1);
            target = parseInt(literal, 16);
        } else {
            target = null;
        }
        return target != null ? new SimpleScript(valueOf(target), databaseInfo) : null;
    }
}

