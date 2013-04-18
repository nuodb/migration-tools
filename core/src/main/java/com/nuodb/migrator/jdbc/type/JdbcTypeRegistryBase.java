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

import java.util.Collection;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeRegistryBase implements JdbcTypeRegistry {

    private Map<JdbcTypeDesc, JdbcType> jdbcTypeMap = Maps.newHashMap();
    private Map<Class, JdbcTypeAdapter> jdbcTypeAdapterMap = Maps.newHashMap();
    private Map<JdbcTypeDesc, JdbcTypeDesc> jdbcTypeDescAliasMap = Maps.newHashMap();

    public JdbcTypeRegistryBase() {
    }

    public JdbcTypeRegistryBase(JdbcTypeRegistry typeRegistry) {
        addJdbcTypes(typeRegistry.getJdbcTypes());
        addJdbcTypeAdapters(typeRegistry.getJdbcTypeAdapters());
        addJdbcTypeDescAliases(typeRegistry.getJdbcTypeDescAliases());
    }

    @Override
    public JdbcType getJdbcType(int typeCode) {
        return getJdbcType(new JdbcTypeDesc(typeCode));
    }

    @Override
    public JdbcType getJdbcType(int typeCode, String typeName) {
        return getJdbcType(new JdbcTypeDesc(typeCode, typeName));
    }

    @Override
    public JdbcType getJdbcType(JdbcTypeDesc jdbcTypeDesc) {
        return findJdbcType(findJdbcTypeDescAlias(jdbcTypeDesc));
    }

    @Override
    public void addJdbcType(JdbcType jdbcType) {
        JdbcTypeDesc typeDesc = jdbcType.getJdbcTypeDesc();
        jdbcTypeMap.put(typeDesc, jdbcType);
    }

    @Override
    public Collection<JdbcType> getJdbcTypes() {
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
    public void addJdbcTypes(JdbcTypeRegistry jdbcTypeRegistry) {
        addJdbcTypes(jdbcTypeRegistry.getJdbcTypes());
    }

    @Override
    public void addJdbcTypes(Collection<JdbcType> jdbcTypes) {
        for (JdbcType jdbcType : jdbcTypes) {
            addJdbcType(jdbcType);
        }
    }

    @Override
    public void addJdbcTypeDescAlias(int typeCode, int typeCodeAlias) {
        addJdbcTypeDescAlias(new JdbcTypeDesc(typeCode, null), new JdbcTypeDesc(typeCodeAlias, null));
    }

    @Override
    public void addJdbcTypeDescAlias(int typeCode, String typeName, int typeCodeAlias) {
        addJdbcTypeDescAlias(new JdbcTypeDesc(typeCode, typeName), new JdbcTypeDesc(typeCodeAlias, typeName));
    }

    @Override
    public void addJdbcTypeDescAlias(JdbcTypeDesc jdbcTypeDesc, JdbcTypeDesc jdbcTypeDescAlias) {
        jdbcTypeDescAliasMap.put(jdbcTypeDesc, jdbcTypeDescAlias);
    }

    @Override
    public JdbcTypeDesc getJdbcTypeDescAlias(int typeCode) {
        return getJdbcTypeDescAlias(new JdbcTypeDesc(typeCode, null));
    }

    @Override
    public JdbcTypeDesc getJdbcTypeDescAlias(int typeCode, String typeName) {
        return getJdbcTypeDescAlias(new JdbcTypeDesc(typeCode, typeName));
    }

    @Override
    public JdbcTypeDesc getJdbcTypeDescAlias(JdbcTypeDesc jdbcTypeDesc) {
        return findJdbcTypeDescAlias(jdbcTypeDesc);
    }

    @Override
    public Map<JdbcTypeDesc, JdbcTypeDesc> getJdbcTypeDescAliases() {
        return jdbcTypeDescAliasMap;
    }

    protected JdbcType findJdbcType(JdbcTypeDesc jdbcTypeDesc) {
        JdbcType jdbcType = jdbcTypeMap.get(jdbcTypeDesc);
        if (jdbcType == null) {
            jdbcType = jdbcTypeMap.get(new JdbcTypeDesc(jdbcTypeDesc.getTypeCode()));
        }
        return jdbcType;
    }

    protected void addJdbcTypeDescAliases(Map<JdbcTypeDesc, JdbcTypeDesc> jdbcTypeDescAliases) {
        for (Map.Entry<JdbcTypeDesc, JdbcTypeDesc> entry : jdbcTypeDescAliases.entrySet()) {
            addJdbcTypeDescAlias(entry.getKey(), entry.getValue());
        }
    }

    protected JdbcTypeDesc findJdbcTypeDescAlias(JdbcTypeDesc jdbcTypeDesc) {
        JdbcTypeDesc jdbcTypeDescAlias = jdbcTypeDescAliasMap.get(jdbcTypeDesc);
        if (jdbcTypeDescAlias == null) {
            jdbcTypeDescAlias = jdbcTypeDescAliasMap.get(new JdbcTypeDesc(jdbcTypeDesc.getTypeCode()));
        }
        if (jdbcTypeDescAlias == null) {
            jdbcTypeDescAlias = jdbcTypeDesc;
        }
        return jdbcTypeDescAlias;
    }
}
