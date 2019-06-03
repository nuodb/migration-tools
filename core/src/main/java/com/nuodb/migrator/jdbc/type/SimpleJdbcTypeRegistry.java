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

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleJdbcTypeRegistry implements JdbcTypeRegistry {

    private Map<JdbcTypeDesc, JdbcTypeValue> jdbcTypeMap = Maps.newHashMap();
    private Map<Class, JdbcTypeAdapter> jdbcTypeAdapterMap = Maps.newHashMap();
    private Map<JdbcTypeDesc, JdbcTypeDesc> jdbcTypeAliasMap = Maps.newHashMap();

    public SimpleJdbcTypeRegistry() {
    }

    public SimpleJdbcTypeRegistry(JdbcTypeRegistry jdbcTypeRegistry) {
        addJdbcTypes(jdbcTypeRegistry.getJdbcTypes());
        addJdbcTypeAdapters(jdbcTypeRegistry.getJdbcTypeAdapters());
        addJdbcTypeAliases(jdbcTypeRegistry.getJdbcTypeAliases());
    }

    @Override
    public JdbcTypeValue getJdbcType(int typeCode) {
        return getJdbcType(new JdbcTypeDesc(typeCode));
    }

    @Override
    public JdbcTypeValue getJdbcType(int typeCode, String typeName) {
        return getJdbcType(new JdbcTypeDesc(typeCode, typeName));
    }

    @Override
    public JdbcTypeValue getJdbcType(JdbcTypeDesc jdbcTypeDesc) {
        return getJdbcType(jdbcTypeDesc, false);
    }

    @Override
    public JdbcTypeValue getJdbcType(JdbcTypeDesc jdbcTypeDesc, boolean required) {
        JdbcTypeValue jdbcTypeValue = findJdbcType(findJdbcTypeAlias(jdbcTypeDesc));
        if (jdbcTypeValue == null && required) {
            throw new JdbcTypeException(format("Jdbc type %s, code(%d) is not supported", jdbcTypeDesc.getTypeName(),
                    jdbcTypeDesc.getTypeCode()));
        }
        return jdbcTypeValue;
    }

    @Override
    public void addJdbcType(JdbcTypeValue jdbcTypeValue) {
        JdbcTypeDesc typeDesc = jdbcTypeValue.getJdbcTypeDesc();
        jdbcTypeMap.put(typeDesc, jdbcTypeValue);
    }

    @Override
    public Collection<JdbcTypeValue> getJdbcTypes() {
        return jdbcTypeMap.values();
    }

    @Override
    public void addJdbcTypeAdapter(JdbcTypeAdapter jdbcTypeAdapter) {
        jdbcTypeAdapterMap.put(jdbcTypeAdapter.getTypeClass(), jdbcTypeAdapter);
    }

    @Override
    public void addJdbcTypeAdapters(Collection<JdbcTypeAdapter> jdbcTypeAdapters) {
        for (JdbcTypeAdapter jdbcTypeAdapter : jdbcTypeAdapters) {
            addJdbcTypeAdapter(jdbcTypeAdapter);
        }
    }

    @Override
    public Collection<JdbcTypeAdapter> getJdbcTypeAdapters() {
        return jdbcTypeAdapterMap.values();
    }

    @Override
    public JdbcTypeAdapter getJdbcTypeAdapter(Class valueClass) {
        return jdbcTypeAdapterMap.get(valueClass);
    }

    @Override
    public JdbcTypeAdapter getJdbcTypeAdapter(Class valueClass, Class typeClass) {
        JdbcTypeAdapter jdbcTypeAdapter = null;
        if (valueClass != null && !typeClass.isAssignableFrom(valueClass)) {
            jdbcTypeAdapter = getJdbcTypeAdapter(typeClass);
            if (jdbcTypeAdapter == null) {
                throw new JdbcTypeException(format("Jdbc type %s adapter not found to adapt %s type",
                        typeClass.getName(), valueClass.getName()));
            }
        }
        return jdbcTypeAdapter;
    }

    @Override
    public void addJdbcTypes(JdbcTypeRegistry jdbcTypeRegistry) {
        addJdbcTypes(jdbcTypeRegistry.getJdbcTypes());
    }

    @Override
    public void addJdbcTypes(Collection<JdbcTypeValue> jdbcTypeValues) {
        for (JdbcTypeValue jdbcTypeValue : jdbcTypeValues) {
            addJdbcType(jdbcTypeValue);
        }
    }

    @Override
    public void addJdbcTypeAlias(int typeCode, int typeAlias) {
        addJdbcTypeAlias(new JdbcTypeDesc(typeCode), new JdbcTypeDesc(typeAlias));
    }

    @Override
    public void addJdbcTypeAlias(int typeCode, String typeName, int typeAlias) {
        addJdbcTypeAlias(new JdbcTypeDesc(typeCode, typeName), new JdbcTypeDesc(typeAlias, typeName));
    }

    @Override
    public void addJdbcTypeAlias(JdbcTypeDesc jdbcTypeDesc, int typeAlias) {
        addJdbcTypeAlias(jdbcTypeDesc, new JdbcTypeDesc(typeAlias));
    }

    @Override
    public void addJdbcTypeAlias(JdbcTypeDesc jdbcTypeDesc, JdbcTypeDesc jdbcTypeAlias) {
        jdbcTypeAliasMap.put(jdbcTypeDesc, jdbcTypeAlias);
    }

    @Override
    public JdbcTypeDesc getJdbcTypeAlias(int typeCode) {
        return getJdbcTypeAlias(new JdbcTypeDesc(typeCode, null));
    }

    @Override
    public JdbcTypeDesc getJdbcTypeAlias(int typeCode, String typeName) {
        return getJdbcTypeAlias(new JdbcTypeDesc(typeCode, typeName));
    }

    @Override
    public JdbcTypeDesc getJdbcTypeAlias(JdbcTypeDesc jdbcTypeDesc) {
        return findJdbcTypeAlias(jdbcTypeDesc);
    }

    @Override
    public Map<JdbcTypeDesc, JdbcTypeDesc> getJdbcTypeAliases() {
        return jdbcTypeAliasMap;
    }

    protected JdbcTypeValue findJdbcType(JdbcTypeDesc jdbcTypeDesc) {
        JdbcTypeValue jdbcTypeValue = jdbcTypeMap.get(jdbcTypeDesc);
        if (jdbcTypeValue == null) {
            jdbcTypeValue = jdbcTypeMap.get(new JdbcTypeDesc(jdbcTypeDesc.getTypeCode()));
        }
        return jdbcTypeValue;
    }

    protected void addJdbcTypeAliases(Map<JdbcTypeDesc, JdbcTypeDesc> jdbcTypeAliases) {
        for (Map.Entry<JdbcTypeDesc, JdbcTypeDesc> entry : jdbcTypeAliases.entrySet()) {
            addJdbcTypeAlias(entry.getKey(), entry.getValue());
        }
    }

    protected JdbcTypeDesc findJdbcTypeAlias(JdbcTypeDesc jdbcTypeDesc) {
        JdbcTypeDesc jdbcTypeAlias = jdbcTypeAliasMap.get(jdbcTypeDesc);
        if (jdbcTypeAlias == null) {
            jdbcTypeAlias = jdbcTypeAliasMap.get(new JdbcTypeDesc(jdbcTypeDesc.getTypeCode()));
        }
        if (jdbcTypeAlias == null) {
            jdbcTypeAlias = jdbcTypeDesc;
        }
        return jdbcTypeAlias;
    }
}
