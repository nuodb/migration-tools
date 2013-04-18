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
package com.nuodb.migrator.jdbc.type;

import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers.newSpecifiers;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.quote;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeNameMap {

    public static final String SIZE = "N";
    public static final String PRECISION = "P";
    public static final String SCALE = "S";
    public static final String VARIABLE_PREFIX = "{";
    public static final String VARIABLE_SUFFIX = "}";

    private Map<JdbcTypeDesc, Map<JdbcTypeSpecifiers, String>> jdbcTypeDescMap = Maps.newHashMap();

    public void addJdbcTypeName(int typeCode, String typeName) {
        addJdbcTypeName(new JdbcTypeDesc(typeCode), typeName);
    }

    public void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, String typeName) {
        addJdbcTypeName(jdbcTypeDesc, typeName, null);
    }

    public void addJdbcTypeName(int typeCode, String typeName, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        addJdbcTypeName(new JdbcTypeDesc(typeCode), typeName, jdbcTypeSpecifiers);
    }

    public void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, String typeName, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> jdbcTypeSpecifiersMap = jdbcTypeDescMap.get(jdbcTypeDesc);
        if (jdbcTypeSpecifiersMap == null) {
            jdbcTypeDescMap.put(jdbcTypeDesc, jdbcTypeSpecifiersMap = Maps.newHashMap());
        }
        jdbcTypeSpecifiersMap.put(jdbcTypeSpecifiers, typeName);
    }

    public String getJdbcTypeName(JdbcTypeDesc jdbcTypeDesc) {
        Map<JdbcTypeSpecifiers, String> jdbcTypeSpecifiersMap = jdbcTypeDescMap.get(jdbcTypeDesc);
        return jdbcTypeSpecifiersMap != null ? jdbcTypeSpecifiersMap.get(null) : null;
    }

    public String getJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, int size, int precision, int scale) {
        return getJdbcTypeName(jdbcTypeDesc, newSpecifiers(size, precision, scale));
    }

    public String getJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> jdbcTypeSpecifiersMap = jdbcTypeDescMap.get(jdbcTypeDesc);
        String jdbcTypeName = getJdbcTypeName(jdbcTypeDesc);
        if (jdbcTypeSpecifiersMap != null) {
            JdbcTypeSpecifiers targetJdbcTypeSpecifiers = jdbcTypeSpecifiers;
            for (Map.Entry<JdbcTypeSpecifiers, String> jdbcTypeSpecifiersEntry : jdbcTypeSpecifiersMap.entrySet()) {
                String entryJdbcTypeName = jdbcTypeSpecifiersEntry.getValue();
                JdbcTypeSpecifiers entryJdbcTypeSpecifiers = jdbcTypeSpecifiersEntry.getKey();
                if (entryJdbcTypeSpecifiers == null) {
                    continue;
                }
                int entryJdbcTypeSpecifiersOrder = compareJdbcTypeSpecifiers(jdbcTypeSpecifiers,
                        entryJdbcTypeSpecifiers);
                if (entryJdbcTypeSpecifiersOrder == 0) {
                    jdbcTypeName = entryJdbcTypeName;
                    break;
                }
                int targetJdbcTypeSpecifiersOrder = compareJdbcTypeSpecifiers(entryJdbcTypeSpecifiers,
                        targetJdbcTypeSpecifiers);
                if (entryJdbcTypeSpecifiersOrder > 0 && targetJdbcTypeSpecifiersOrder >= 0) {
                    jdbcTypeName = entryJdbcTypeName;
                    targetJdbcTypeSpecifiers = entryJdbcTypeSpecifiers;
                }
            }
        }
        return compileJdbcTypeName(jdbcTypeName, jdbcTypeSpecifiers);
    }

    protected String compileJdbcTypeName(String jdbcTypeName, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        if (!StringUtils.isEmpty(jdbcTypeName)) {
            Integer size = jdbcTypeSpecifiers.getSize();
            Integer precision = jdbcTypeSpecifiers.getPrecision();
            Integer scale = jdbcTypeSpecifiers.getScale();
            jdbcTypeName = compileJdbcTypeName(jdbcTypeName, SIZE,
                    size != null ? Integer.toString(size) : null);
            jdbcTypeName = compileJdbcTypeName(jdbcTypeName, PRECISION,
                    precision != null ? Integer.toString(precision) : null);
            jdbcTypeName = compileJdbcTypeName(jdbcTypeName, SCALE,
                    scale != null ? Integer.toString(scale) : null);
        }
        return jdbcTypeName;
    }

    protected String compileJdbcTypeName(String typeName, String variable, String value) {
        Pattern pattern = Pattern.compile(quote(VARIABLE_PREFIX + variable + VARIABLE_SUFFIX), CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(typeName);
        return value != null && matcher.find() ? matcher.replaceAll(value) : typeName;
    }

    protected int compareJdbcTypeSpecifiers(JdbcTypeSpecifiers jdbcTypeSpecifiers1,
                                            JdbcTypeSpecifiers jdbcTypeSpecifiers2) {
        return JDBC_TYPE_SPECIFIERS_COMPARATOR.compare(jdbcTypeSpecifiers1, jdbcTypeSpecifiers2);
    }

    public void removeJdbcTypeName(int typeCode) {
        removeJdbcTypeName(new JdbcTypeDesc(typeCode));
    }

    public void removeJdbcTypeName(JdbcTypeDesc jdbcTypeDesc) {
        removeJdbcTypeName(jdbcTypeDesc, null);
    }

    public void removeJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> jdbcTypeSpecifiersMap = jdbcTypeDescMap.get(jdbcTypeDesc);
        if (jdbcTypeSpecifiersMap != null) {
            jdbcTypeSpecifiersMap.remove(jdbcTypeSpecifiers);
        }
    }

    private static final Comparator<JdbcTypeSpecifiers> JDBC_TYPE_SPECIFIERS_COMPARATOR =
            new Ordering<JdbcTypeSpecifiers>() {
                @Override
                public int compare(JdbcTypeSpecifiers t1, JdbcTypeSpecifiers t2) {
                    int result = compare(t1.getSize(), t2.getSize());
                    if (result == 0) {
                        result = compare(t1.getPrecision(), t2.getPrecision());
                    }
                    if (result == 0) {
                        result = compare(t1.getScale(), t2.getScale());
                    }
                    return result;
                }

                private int compare(Integer i1, Integer i2) {
                    return i1 != null && i2 != null ? i1.compareTo(i2) : 0;
                }

            }.nullsFirst();
}