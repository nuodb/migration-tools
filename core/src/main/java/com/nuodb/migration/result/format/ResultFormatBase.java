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
package com.nuodb.migration.result.format;

import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.model.ColumnModelSet;
import com.nuodb.migration.jdbc.type.JdbcType;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessProvider;
import com.nuodb.migration.result.format.jdbc.JdbcTypeValueFormat;
import com.nuodb.migration.result.format.jdbc.JdbcTypeValueFormatImpl;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
public abstract class ResultFormatBase implements ResultFormat {

    private Map<String, String> attributes;
    private Map<Integer, JdbcTypeValueFormat> jdbcTypeValueFormats = Maps.newHashMap();
    private JdbcTypeValueFormat defaultJdbcTypeValueFormat = new JdbcTypeValueFormatImpl();
    private JdbcTypeValueAccessProvider jdbcTypeValueAccessProvider;
    private ColumnModelSet columnModelSet;

    @Override
    public final void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String getAttribute(String attribute) {
        return attributes.get(attribute);
    }

    @Override
    public String getAttribute(String attribute, String defaultValue) {
        String value = null;
        if (attributes != null) {
            value = attributes.get(attribute);
        }
        return value == null ? defaultValue : value;
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public void addJdbcTypeValueFormat(int jdbcTypeCode, JdbcTypeValueFormat jdbcTypeValueFormat) {
        jdbcTypeValueFormats.put(jdbcTypeCode, jdbcTypeValueFormat);
    }

    @Override
    public void addJdbcTypeValueFormat(JdbcType jdbcType, JdbcTypeValueFormat jdbcTypeValueFormat) {
        addJdbcTypeValueFormat(jdbcType.getTypeCode(), jdbcTypeValueFormat);
    }

    @Override
    public JdbcTypeValueFormat getJdbcTypeValueFormat(JdbcType jdbcType) {
        return getJdbcTypeValueFormat(jdbcType.getTypeCode());
    }

    @Override
    public JdbcTypeValueFormat getJdbcTypeValueFormat(int typeCode) {
        JdbcTypeValueFormat jdbcTypeValueFormat = jdbcTypeValueFormats.get(typeCode);
        if (jdbcTypeValueFormat == null) {
            jdbcTypeValueFormat = getDefaultJdbcTypeValueFormat();
        }
        return jdbcTypeValueFormat;
    }

    @Override
    public JdbcTypeValueFormat getDefaultJdbcTypeValueFormat() {
        return defaultJdbcTypeValueFormat;
    }

    @Override
    public void setDefaultJdbcTypeValueFormat(JdbcTypeValueFormat defaultJdbcTypeValueFormat) {
        this.defaultJdbcTypeValueFormat = defaultJdbcTypeValueFormat;
    }

    @Override
    public JdbcTypeValueAccessProvider getJdbcTypeValueAccessProvider() {
        return jdbcTypeValueAccessProvider;
    }

    @Override
    public void setJdbcTypeValueAccessProvider(JdbcTypeValueAccessProvider jdbcTypeValueAccessProvider) {
        this.jdbcTypeValueAccessProvider = jdbcTypeValueAccessProvider;
    }

    @Override
    public ColumnModelSet getColumnModelSet() {
        return columnModelSet;
    }

    @Override
    public void setColumnModelSet(ColumnModelSet columnModelSet) {
        this.columnModelSet = columnModelSet;
    }
}
