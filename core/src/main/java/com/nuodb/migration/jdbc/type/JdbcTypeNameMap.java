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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeNameMap {

    public static final JdbcTypeNameMap STANDARD = new StandardJdbcTypeNameMap();

    public static final String LENGTH = "n";
    public static final String PRECISION = "p";
    public static final String SCALE = "s";
    public static final String VARIABLE_PREFIX = "{";
    public static final String VARIABLE_SUFFIX = "}";

    private Map<Integer, String> typeCodeMap = Maps.newHashMap();
    private Map<Integer, Map<Integer, String>> typeCodeLengthMap = Maps.newHashMap();

    public void addJdbcTypeName(int typeCode, String typeName) {
        typeCodeMap.put(typeCode, typeName);
    }

    public void addJdbcTypeName(int typeCode, String typeName, int length) {
        Map<Integer, String> typeLengthMap = typeCodeLengthMap.get(typeCode);
        if (typeLengthMap == null) {
            typeLengthMap = Maps.newTreeMap();
            typeCodeLengthMap.put(typeCode, typeLengthMap);
        }
        typeLengthMap.put(length, typeName);
    }

    public void removeJdbcTypeName(int typeCode) {
        typeCodeMap.remove(typeCode);
    }

    public String getJdbcTypeName(int typeCode) {
        return typeCodeMap.get(typeCode);
    }

    public String getJdbcTypeName(int typeCode, int length, int precision, int scale) {
        Map<Integer, String> map = typeCodeLengthMap.get(typeCode);
        if (map != null) {
            for (Map.Entry<Integer, String> entry : map.entrySet()) {
                if (length <= entry.getKey()) {
                    return expandVariables(entry.getValue(), length, precision, scale);
                }
            }
        }
        return expandVariables(getJdbcTypeName(typeCode), length, precision, scale);
    }

    protected String expandVariables(String typeName, int length, int precision, int scale) {
        if (!StringUtils.isEmpty(typeName)) {
            typeName = expandVariable(typeName, LENGTH, Integer.toString(length));
            typeName = expandVariable(typeName, PRECISION, Integer.toString(precision));
            typeName = expandVariable(typeName, SCALE, Integer.toString(scale));
        }
        return typeName;
    }

    protected String expandVariable(String typeName, String variable, String value) {
        Pattern compile = Pattern.compile(
                Pattern.quote(VARIABLE_PREFIX + variable + VARIABLE_SUFFIX), Pattern.CASE_INSENSITIVE);
        Matcher matcher = compile.matcher(typeName);
        return matcher.find() ? matcher.replaceAll(value) : typeName;
    }

    static class StandardJdbcTypeNameMap extends JdbcTypeNameMap {

        private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTypeNameMap.class);

        private static Map<Integer, String> createJdbcTypeNameMap() {
            Map<Integer, String> typeNameMap = Maps.newLinkedHashMap();
            Field[] fields = Types.class.getFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers()) && field.getType() == int.class) {
                    try {
                        typeNameMap.put((Integer) field.get(null), field.getName());
                    } catch (IllegalAccessException exception) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Failed accessing %s field", Types.class), exception);
                        }
                    }
                }
            }
            return typeNameMap;
        }

        private StandardJdbcTypeNameMap() {
            for (Map.Entry<Integer, String> entry : createJdbcTypeNameMap().entrySet()) {
                addJdbcTypeName(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public String getJdbcTypeName(int typeCode, int length, int precision, int scale) {
            String typeName = super.getJdbcTypeName(typeCode, length, precision, scale);
            return typeName != null ? typeName : "type(" + typeCode + ")";
        }
    }
}
