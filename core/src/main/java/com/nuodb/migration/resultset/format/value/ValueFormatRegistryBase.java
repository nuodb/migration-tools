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
package com.nuodb.migration.resultset.format.value;

import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.type.JdbcType;
import com.nuodb.migration.jdbc.type.JdbcTypeDesc;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class ValueFormatRegistryBase implements ValueFormatRegistry {

    private Map<JdbcTypeDesc, ValueFormat> valueFormats = Maps.newHashMap();
    private ValueFormat defaultValueFormat;

    public ValueFormatRegistryBase() {
    }

    public ValueFormatRegistryBase(ValueFormat defaultValueFormat) {
        this.defaultValueFormat = defaultValueFormat;
    }

    @Override
    public void addValueFormat(JdbcType jdbcType, ValueFormat valueFormat) {
        addValueFormat(jdbcType.getTypeDesc(), valueFormat);
    }

    @Override
    public void addValueFormat(JdbcTypeDesc jdbcTypeDesc, ValueFormat valueFormat) {
        valueFormats.put(jdbcTypeDesc, valueFormat);
    }

    @Override
    public ValueFormat getValueFormat(JdbcTypeDesc jdbcTypeDesc) {
        ValueFormat valueFormat = valueFormats.get(jdbcTypeDesc);
        if (valueFormat == null) {
            valueFormat = valueFormats.get(new JdbcTypeDesc(jdbcTypeDesc.getTypeCode()));
        }
        if (valueFormat == null) {
            valueFormat = getDefaultValueFormat();
        }
        return valueFormat;
    }

    @Override
    public Map<JdbcTypeDesc, ValueFormat> getValueFormats() {
        return valueFormats;
    }

    public ValueFormat getDefaultValueFormat() {
        return defaultValueFormat;
    }

    public void setDefaultValueFormat(ValueFormat defaultValueFormat) {
        this.defaultValueFormat = defaultValueFormat;
    }
}
