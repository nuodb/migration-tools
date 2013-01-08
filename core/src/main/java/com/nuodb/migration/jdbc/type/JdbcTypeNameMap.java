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

    private Map<Integer, Map<JdbcTypeSpecifiers, String>> typeCodeTypeSpecifiersMap = Maps.newHashMap();

    public void addTypeName(int typeCode, String typeName) {
        addTypeName(typeCode, typeName, null);
    }

    public void addTypeName(int typeCode, String typeName, JdbcTypeSpecifiers typeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeCodeTypeSpecifiersMap.get(typeCode);
        if (typeSpecifiersMap == null) {
            typeSpecifiersMap = Maps.newHashMap();
            typeCodeTypeSpecifiersMap.put(typeCode, typeSpecifiersMap);
        }
        typeSpecifiersMap.put(typeSpecifiers, typeName.toUpperCase());
    }

    public String getTypeName(int typeCode) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeCodeTypeSpecifiersMap.get(typeCode);
        return typeSpecifiersMap != null ? typeSpecifiersMap.get(null) : null;
    }

    public String getTypeName(int typeCode, int size, int precision, int scale) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeCodeTypeSpecifiersMap.get(typeCode);
        String typeName = null;
        if (typeSpecifiersMap != null) {
            JdbcTypeSpecifiers targetTypeSpecifiers = newSizePrecisionScale(size, precision, scale);
            for (Map.Entry<JdbcTypeSpecifiers, String> entry : typeSpecifiersMap.entrySet()) {
                JdbcTypeSpecifiers typeSpecifiers = entry.getKey();
                if (TYPE_SPECIFIERS_COMPARATOR.compare(typeSpecifiers, targetTypeSpecifiers) >= 0) {
                    if (typeSpecifiers != null) {
                        targetTypeSpecifiers = typeSpecifiers;
                    }
                    typeName = entry.getValue();
                }
            }
        }
        if (typeName == null) {
            typeName = getTypeName(typeCode);
        }
        return expand(typeName, size, precision, scale);
    }

    public void removeTypeName(int typeCode) {
        removeTypeName(typeCode, null);
    }

    public void removeTypeName(int typeCode, JdbcTypeSpecifiers typeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeCodeTypeSpecifiersMap.get(typeCode);
        if (typeSpecifiersMap != null) {
            typeSpecifiersMap.remove(typeSpecifiers);
        }
    }

    private static String expand(String template, int size, int precision, int scale) {
        if (!StringUtils.isEmpty(template)) {
            template = expand(template, SIZE, Integer.toString(size));
            template = expand(template, PRECISION, Integer.toString(precision));
            template = expand(template, SCALE, Integer.toString(scale));
        }
        return template;
    }

    private static String expand(String template, String variable, String value) {
        Pattern compile = Pattern.compile(
                Pattern.quote(VARIABLE_PREFIX + variable + VARIABLE_SUFFIX), Pattern.CASE_INSENSITIVE);
        Matcher matcher = compile.matcher(template);
        return matcher.find() ? matcher.replaceAll(value) : template;
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