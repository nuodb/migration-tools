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
import com.nuodb.migrator.jdbc.type.JdbcType;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class NuoDBColumn {

    /**
     * Create derived enum type or returns base type if type argument is null
     *
     * @param baseType
     *            base type to use
     * @param type
     *            definition of enumeration
     * @return base type or enum type if enumeration values are provided
     */
    public static JdbcType getJdbcType(JdbcType baseType, String type) {
        JdbcType jdbcType = baseType;
        if (type != null) {
            jdbcType = new JdbcEnumType(baseType, getEnum(type));
        }
        return jdbcType;
    }

    /**
     * ENUM type decoding rules:
     * <ul>
     * <li>^ delimiter character</li>
     * <li>\ escape symbol</li>
     * <li>\^ turns into ^</li>
     * <li>\\ is changed to \</li>
     * </ul>
     *
     * @param type
     *            serialized set of enum values
     * @return decoded set of enum values
     */
    public static Collection<String> getEnum(String type) {
        boolean start = false;
        boolean escape = false;
        char[] symbols = type.toCharArray();
        StringBuilder value = new StringBuilder();
        Collection<String> values = newArrayList();
        for (int index = 0; index < symbols.length; index++) {
            char symbol = symbols[index];
            if (escape) {
                value.append(symbol);
                escape = false;
                continue;
            }
            switch (symbol) {
            case '\\':
                escape = true;
                break;
            case '^':
                if (start) {
                    values.add(value.toString());
                    value.setLength(0);
                }
                if (!start) {
                    start = true;
                }
                break;
            default:
                value.append(symbol);
                break;
            }
        }
        return values;
    }
}
