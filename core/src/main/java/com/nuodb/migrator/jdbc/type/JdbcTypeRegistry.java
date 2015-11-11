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

import java.util.Collection;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public interface JdbcTypeRegistry {

    JdbcTypeValue getJdbcType(int typeCode);

    JdbcTypeValue getJdbcType(int typeCode, String typeName);

    JdbcTypeValue getJdbcType(JdbcTypeDesc jdbcTypeDesc);

    JdbcTypeValue getJdbcType(JdbcTypeDesc jdbcTypeDesc, boolean required);

    void addJdbcType(JdbcTypeValue type);

    void addJdbcTypes(Collection<JdbcTypeValue> jdbcTypeValues);

    void addJdbcTypes(JdbcTypeRegistry jdbcTypeRegistry);

    Collection<JdbcTypeValue> getJdbcTypes();

    JdbcTypeAdapter getJdbcTypeAdapter(Class valueClass);

    JdbcTypeAdapter getJdbcTypeAdapter(Class valueClass, Class typeClass);

    void addJdbcTypeAdapter(JdbcTypeAdapter jdbcTypeAdapter);

    void addJdbcTypeAdapters(Collection<JdbcTypeAdapter> jdbcTypeAdapters);

    Collection<JdbcTypeAdapter> getJdbcTypeAdapters();

    JdbcTypeDesc getJdbcTypeAlias(int typeCode);

    JdbcTypeDesc getJdbcTypeAlias(int typeCode, String typeName);

    JdbcTypeDesc getJdbcTypeAlias(JdbcTypeDesc jdbcTypeDesc);

    void addJdbcTypeAlias(int typeCode, int typeAlias);

    void addJdbcTypeAlias(int typeCode, String typeName, int typeAlias);

    void addJdbcTypeAlias(JdbcTypeDesc jdbcTypeDesc, int typeAlias);

    void addJdbcTypeAlias(JdbcTypeDesc jdbcTypeDesc, JdbcTypeDesc jdbcTypeAlias);

    Map<JdbcTypeDesc, JdbcTypeDesc> getJdbcTypeAliases();
}
