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

import java.sql.Types;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers.newPrecision;
import static com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers.newSpecifiers;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeNameMap {

    public static final String SIZE = "N";
    public static final String PRECISION = "P";
    public static final String SCALE = "S";
    public static final String VARIABLE_PREFIX = "{";
    public static final String VARIABLE_SUFFIX = "}";

    private Map<JdbcTypeDesc, Map<JdbcTypeSpecifiers, String>> typeDescTypeSpecifiersMap = Maps.newHashMap();

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
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeDescTypeSpecifiersMap.get(typeDesc);
        if (typeSpecifiersMap == null) {
            typeDescTypeSpecifiersMap.put(typeDesc, typeSpecifiersMap = Maps.newHashMap());
        }
        typeSpecifiersMap.put(typeSpecifiers, typeName);
    }

    public String getTypeName(JdbcTypeDesc typeDesc) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeDescTypeSpecifiersMap.get(typeDesc);
        return typeSpecifiersMap != null ? typeSpecifiersMap.get(null) : null;
    }

    public String getTypeName(JdbcTypeDesc typeDesc, int size, int precision, int scale) {
        return getTypeName(typeDesc, newSpecifiers(size, precision, scale));
    }

    public String getTypeName(JdbcTypeDesc typeDesc, JdbcTypeSpecifiers typeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeDescTypeSpecifiersMap.get(typeDesc);
        String targetTypeName = getTypeName(typeDesc);
        JdbcTypeSpecifiers targetTypeSpecifiers = typeSpecifiers;
        if (typeSpecifiersMap != null) {
            for (Map.Entry<JdbcTypeSpecifiers, String> typeSpecifiersEntry : typeSpecifiersMap.entrySet()) {
                String entryTypeName = typeSpecifiersEntry.getValue();
                JdbcTypeSpecifiers entryTypeSpecifiers = typeSpecifiersEntry.getKey();
                if (entryTypeSpecifiers == null) {
                    continue;
                }
                int entryTypeSpecifierOrder = compareTypeSpecifiers(typeSpecifiers, entryTypeSpecifiers);
                if (entryTypeSpecifierOrder == 0) {
                    targetTypeName = entryTypeName;
                    break;
                }
                int targetTypeSpecifierOrder = compareTypeSpecifiers(entryTypeSpecifiers, targetTypeSpecifiers);
                if (entryTypeSpecifierOrder > 0 && targetTypeSpecifierOrder >= 0) {
                    targetTypeName = entryTypeName;
                    targetTypeSpecifiers = entryTypeSpecifiers;
                }
            }
        }
        return expandVariables(targetTypeName, typeSpecifiers);
    }

    public void removeTypeName(int typeCode) {
        removeTypeName(new JdbcTypeDesc(typeCode));
    }

    public void removeTypeName(JdbcTypeDesc typeDesc) {
        removeTypeName(typeDesc, null);
    }

    public void removeTypeName(JdbcTypeDesc typeDesc, JdbcTypeSpecifiers typeSpecifiers) {
        Map<JdbcTypeSpecifiers, String> typeSpecifiersMap = typeDescTypeSpecifiersMap.get(typeDesc);
        if (typeSpecifiersMap != null) {
            typeSpecifiersMap.remove(typeSpecifiers);
        }
    }

    protected String expandVariables(String typeName, JdbcTypeSpecifiers typeSpecifiers) {
        if (!StringUtils.isEmpty(typeName)) {
            Integer size = typeSpecifiers.getSize();
            Integer precision = typeSpecifiers.getPrecision();
            Integer scale = typeSpecifiers.getScale();
            typeName = expandVariable(typeName, SIZE, size != null ? Integer.toString(size) : null);
            typeName = expandVariable(typeName, PRECISION, precision != null ? Integer.toString(precision) : null);
            typeName = expandVariable(typeName, SCALE, scale != null ? Integer.toString(scale) : null);
        }
        return typeName;
    }

    protected String expandVariable(String typeName, String variable, String value) {
        Pattern compile = Pattern.compile(
                Pattern.quote(VARIABLE_PREFIX + variable + VARIABLE_SUFFIX), Pattern.CASE_INSENSITIVE);
        Matcher matcher = compile.matcher(typeName);
        return value != null && matcher.find() ? matcher.replaceAll(value) : typeName;
    }

    protected int compareTypeSpecifiers(JdbcTypeSpecifiers t1, JdbcTypeSpecifiers t2) {
        return TYPE_SPECIFIERS_COMPARATOR.compare(t1, t2);
    }

    private static final Comparator<JdbcTypeSpecifiers> TYPE_SPECIFIERS_COMPARATOR = new Ordering<JdbcTypeSpecifiers>() {
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

    public static void main(String[] args) {
        JdbcTypeNameMap typeNameMap = new JdbcTypeNameMap();
        typeNameMap.addTypeName(Types.BIGINT, "BIGINT");
        typeNameMap.addTypeName(Types.BIGINT, "BIGINT_PRECISION_1({P})", newPrecision(1));
        typeNameMap.addTypeName(Types.BIGINT, "BIGINT_PRECISION_8({P})", newPrecision(8));
        typeNameMap.addTypeName(Types.BIGINT, "BIGINT_PRECISION_10({P})", newPrecision(10));
        typeNameMap.addTypeName(Types.BIGINT, "BIGINT_PRECISION_6({P})", newPrecision(6));
        JdbcTypeDesc typeDesc = new JdbcTypeDesc(Types.BIGINT);
        System.out.println(typeNameMap.getTypeName(typeDesc, newSpecifiers(1, 5, 0)));
    }
}