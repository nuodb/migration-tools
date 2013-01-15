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
package com.nuodb.migration.jdbc.type;

import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.nuodb.migration.jdbc.type.JdbcTypeSpecifiers.newSizePrecisionScale;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeNameMap {

    public static final String SIZE = "N";
    public static final String PRECISION = "P";
    public static final String SCALE = "S";
    public static final String VARIABLE_PREFIX = "{";
    public static final String VARIABLE_SUFFIX = "}";

    private Map<JdbcTypeDesc, Map<JdbcTypeSpecifiers, String>> typeCodeTypeSpecifiersMap = Maps.newHashMap();

    public void addTypeName(int typeCode, String typeName) {
        addTypeName(new JdbcTypeDesc(typeCode), typeName);
    }

    public void addTypeName(JdbcTypeDesc typeDesc, String typeName) {
        addTypeName(typeDesc, typeName, null);
    }

    public void addTypeName(int typeCode, String typeName, JdbcTypeSpecifiers typeSpecifiers) {
        addTypeName(new JdbcTypeDesc(typeCode), typeName, typeSpecifiers);
    }

    public void addTypeName(JdbcTypeDesc typeDesc, String typeName, JdbcTypeSpecifiers typeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeCodeTypeSpecifiersMap.get(typeDesc);
        if (typeSpecifiersMap == null) {
            typeSpecifiersMap = Maps.newHashMap();
            typeCodeTypeSpecifiersMap.put(typeDesc, typeSpecifiersMap);
        }
        typeSpecifiersMap.put(typeSpecifiers, typeName.toUpperCase());
    }

    public String getTypeName(JdbcTypeDesc typeDesc) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeCodeTypeSpecifiersMap.get(typeDesc);
        return typeSpecifiersMap != null ? typeSpecifiersMap.get(null) : null;
    }

    public String getTypeName(JdbcTypeDesc typeDesc, int size, int precision, int scale) {
        return getTypeName(typeDesc, newSizePrecisionScale(size, precision, scale));
    }

    public String getTypeName(JdbcTypeDesc typeDesc, JdbcTypeSpecifiers typeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeCodeTypeSpecifiersMap.get(typeDesc);
        String typeName = null;
        if (typeSpecifiersMap != null) {
            for (Map.Entry<JdbcTypeSpecifiers, String> entry : typeSpecifiersMap.entrySet()) {
                JdbcTypeSpecifiers entryTypeSpecifiers = entry.getKey();
                if (TYPE_SPECIFIERS_COMPARATOR.compare(entryTypeSpecifiers, typeSpecifiers) >= 0) {
                    if (typeSpecifiers != null) {
                        typeSpecifiers = entryTypeSpecifiers;
                    }
                    typeName = entry.getValue();
                }
            }
        }
        if (typeName == null) {
            typeName = getTypeName(typeDesc);
        }
        return expand(typeName, typeSpecifiers);
    }

    public void removeTypeName(int typeCode) {
        removeTypeName(new JdbcTypeDesc(typeCode));
    }

    public void removeTypeName(JdbcTypeDesc typeDesc) {
        removeTypeName(typeDesc, null);
    }

    public void removeTypeName(JdbcTypeDesc typeDesc, JdbcTypeSpecifiers typeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeCodeTypeSpecifiersMap.get(typeDesc);
        if (typeSpecifiersMap != null) {
            typeSpecifiersMap.remove(typeSpecifiers);
        }
    }

    private static String expand(String typeName, JdbcTypeSpecifiers typeSpecifiers) {
        if (!StringUtils.isEmpty(typeName)) {
            Integer size = typeSpecifiers.getSize();
            Integer precision = typeSpecifiers.getPrecision();
            Integer scale = typeSpecifiers.getScale();
            typeName = expand(typeName, SIZE, size != null ? Integer.toString(size) : null);
            typeName = expand(typeName, PRECISION, precision != null ? Integer.toString(precision) : null);
            typeName = expand(typeName, SCALE, scale != null ? Integer.toString(scale) : null);
        }
        return typeName;
    }

    private static String expand(String typeName, String variable, String value) {
        Pattern compile = Pattern.compile(
                Pattern.quote(VARIABLE_PREFIX + variable + VARIABLE_SUFFIX), Pattern.CASE_INSENSITIVE);
        Matcher matcher = compile.matcher(typeName);
        return value != null && matcher.find() ? matcher.replaceAll(value) : typeName;
    }

    private static final Comparator<JdbcTypeSpecifiers> TYPE_SPECIFIERS_COMPARATOR = new Ordering<JdbcTypeSpecifiers>() {
        @Override
        public int compare(JdbcTypeSpecifiers left, JdbcTypeSpecifiers right) {
            int result = compare(left.getSize(), right.getSize());
            if (result == 0) {
                result = compare(left.getPrecision(), right.getPrecision());
            }
            if (result == 0) {
                result = compare(left.getScale(), right.getScale());
            }
            return result;
        }

        private int compare(Integer left, Integer right) {
            return left != null && right != null ? left.compareTo(right) : 0;
        }

    }.nullsFirst();
}