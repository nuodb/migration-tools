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

import java.util.Collection;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeRegistryBase implements JdbcTypeRegistry {

    private Map<JdbcTypeDesc, JdbcType> jdbcTypes = Maps.newHashMap();

    private Map<Class, JdbcTypeAdapter> jdbcTypeAdapters = Maps.newHashMap();

    private JdbcTypeNameMap jdbcTypeNameMap = new JdbcTypeNameMap();

    public JdbcTypeRegistryBase() {
    }

    public JdbcTypeRegistryBase(JdbcTypeRegistry jdbcTypeRegistry) {
        addJdbcTypes(jdbcTypeRegistry.getJdbcTypes());
        addJdbcTypeAdapters(jdbcTypeRegistry.getJdbcTypeAdapters());
    }

    @Override
    public JdbcTypeNameMap getJdbcTypeNameMap() {
        return jdbcTypeNameMap;
    }

    @Override
    public JdbcType getJdbcType(int typeCode) {
        return jdbcTypes.get(new JdbcTypeDesc(typeCode));
    }

    @Override
    public JdbcType getJdbcType(int typeCode, String typeName) {
        return getJdbcType(new JdbcTypeDesc(typeCode, typeName));
    }

    @Override
    public JdbcType getJdbcType(JdbcTypeDesc typeDesc) {
        JdbcType jdbcType = jdbcTypes.get(typeDesc);
        if (jdbcType == null) {
            jdbcType = jdbcTypes.get(new JdbcTypeDesc(typeDesc.getTypeCode()));
        }
        return jdbcType;
    }

    @Override
    public void addJdbcType(JdbcType jdbcType) {
        JdbcTypeDesc typeDesc = jdbcType.getTypeDesc();
        jdbcTypes.put(typeDesc, jdbcType);
    }

    @Override
    public Collection<JdbcType> getJdbcTypes() {
        return jdbcTypes.values();
    }

    @Override
    public void addJdbcTypeAdapter(JdbcTypeAdapter jdbcTypeAdapter) {
        jdbcTypeAdapters.put(jdbcTypeAdapter.getTypeClass(), jdbcTypeAdapter);
    }

    @Override
    public void addJdbcTypeAdapters(Collection<JdbcTypeAdapter> jdbcTypeAdapters) {
        for (JdbcTypeAdapter jdbcTypeAdapter : jdbcTypeAdapters) {
            addJdbcTypeAdapter(jdbcTypeAdapter);
        }
    }

    @Override
    public Collection<JdbcTypeAdapter> getJdbcTypeAdapters() {
        return jdbcTypeAdapters.values();
    }

    @Override
    public JdbcTypeAdapter getJdbcTypeAdapter(Class typeClass) {
        return jdbcTypeAdapters.get(typeClass);
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
}
