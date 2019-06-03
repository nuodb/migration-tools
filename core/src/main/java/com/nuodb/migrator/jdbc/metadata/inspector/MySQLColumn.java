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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.type.JdbcEnumType;
import com.nuodb.migrator.jdbc.type.JdbcSetType;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import static java.sql.Types.SMALLINT;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class MySQLColumn {

    public static String ENUM = "ENUM";
    public static String SET = "SET";
    public static String YEAR = "YEAR";

    public static JdbcType getJdbcType(JdbcType baseType, String type) {
        JdbcType jdbcType;
        if (ENUM.equals(baseType.getTypeName())) {
            jdbcType = new JdbcEnumType(baseType, getEnum(baseType, type));
        } else if (SET.equals(baseType.getTypeName())) {
            jdbcType = new JdbcSetType(baseType, getSet(baseType, type));
        } else if (YEAR.equals(baseType.getTypeName())) {
            jdbcType = new JdbcType(new JdbcTypeDesc(SMALLINT, "SMALLINT"), newOptions(0, 0, 0));
        } else {
            jdbcType = baseType;
        }
        return jdbcType;
    }

    public static Collection<String> getEnum(JdbcType baseType, String type) {
        return ENUM.equals(baseType.getTypeName()) ? getValues(type.substring(5, type.length() - 1)) : null;
    }

    public static Collection<String> getSet(JdbcType baseType, String type) {
        return SET.equals(baseType.getTypeName()) ? getValues(type.substring(4, type.length() - 1)) : null;
    }

    public static Collection<String> getValues(String type) {
        Collection<String> values = newArrayList();
        int count = 0;
        boolean start = false;
        boolean escape = false;
        int startIndex = 0;
        int index = 0;
        StringBuilder value = new StringBuilder();
        int length = type.length();
        char lastSymbol = 0;
        while (index < length) {
            char symbol = type.charAt(index);
            boolean addSymbol = false;
            boolean addValue = false;
            switch (symbol) {
            case '\'':
                if (start && (lastSymbol == '\'' && index > startIndex)) {
                    addSymbol = true;
                }
                if (!start) {
                    start = true;
                    startIndex = index + 1;
                    count++;
                }
                break;
            case ',':
                addSymbol = escape;
                if (start && !escape) {
                    addValue = true;
                    start = false;
                }
                break;
            case ' ':
                addSymbol = start;
                break;
            case '\\':
                addSymbol = escape;
                escape = !escape;
                break;
            default:
                addSymbol = true;
                break;
            }
            if (addSymbol) {
                value.append(symbol);
            }
            if (addValue) {
                values.add(value.toString());
                value.setLength(0);
                start = false;
            }
            index++;
            lastSymbol = symbol;
        }
        if (count > values.size()) {
            values.add(value.toString());
        }
        return values;
    }
}
