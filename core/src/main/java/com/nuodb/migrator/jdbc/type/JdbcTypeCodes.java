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
package com.nuodb.migrator.jdbc.type;

import com.nuodb.migrator.utils.ObjectUtils;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;

import static com.nuodb.migrator.utils.ReflectionUtils.loadClass;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isStatic;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeCodes {

    private static transient JdbcTypeCodes INSTANCE;
    private static final String TYPES_CLASS_NAME = "java.sql.Types";

    private transient Logger logger = getLogger(getClass());
    private Map<String, Integer> typeCodes = new TreeMap<String, Integer>(CASE_INSENSITIVE_ORDER);

    public static JdbcTypeCodes getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new JdbcTypeCodes();
        }
        return INSTANCE;
    }

    private JdbcTypeCodes() {
        addTypeCodes();
    }

    private void addTypeCodes() {
        Class<?> types = loadClass(TYPES_CLASS_NAME);
        for (Field field : types.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (isStatic(modifiers) && isFinal(modifiers)) {
                int typeCode;
                try {
                    typeCode = field.getInt(null);
                } catch (IllegalAccessException exception) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Can't retrieve type code", exception);
                    }
                    continue;
                }
                String typeName = field.getName();
                addTypeCode(typeName, typeCode);
                addTypeCode(TYPES_CLASS_NAME + "." + typeName, typeCode);
            }
        }
    }

    public void addTypeCode(String typeName, int typeCode) {
        typeCodes.put(typeName, typeCode);
    }

    public Integer getTypeCode(String typeName) {
        return typeCodes.get(typeName);
    }

    public Map<String, Integer> getTypeCodes() {
        return typeCodes;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
