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
package com.nuodb.tools.migration.result.format;

import com.google.common.collect.Maps;
import com.nuodb.tools.migration.jdbc.metamodel.ColumnSetModel;
import com.nuodb.tools.migration.jdbc.type.JdbcType;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeAccessor;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
public abstract class ResultFormatBase implements ResultFormat {

    private Map<String, String> attributes;
    private Map<Integer, JdbcTypeFormat> jdbcTypeFormats = Maps.newHashMap();
    private JdbcTypeFormat defaultJdbcTypeFormat = new JdbcTypeFormatImpl();
    private JdbcTypeAccessor jdbcTypeAccessor;
    private ColumnSetModel columnSetModel;

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
    public void addJdbcTypeFormat(int jdbcTypeCode, JdbcTypeFormat jdbcTypeFormat) {
        jdbcTypeFormats.put(jdbcTypeCode, jdbcTypeFormat);
    }

    @Override
    public void addJdbcTypeFormat(JdbcType jdbcType, JdbcTypeFormat jdbcTypeFormat) {
        addJdbcTypeFormat(jdbcType.getTypeCode(), jdbcTypeFormat);
    }

    @Override
    public JdbcTypeFormat getJdbcTypeFormat(JdbcType jdbcType) {
        return getJdbcTypeFormat(jdbcType.getTypeCode());
    }

    @Override
    public JdbcTypeFormat getJdbcTypeFormat(int jdbcTypeCode) {
        JdbcTypeFormat jdbcTypeFormat = jdbcTypeFormats.get(jdbcTypeCode);
        if (jdbcTypeFormat == null) {
            jdbcTypeFormat = getDefaultJdbcTypeFormat();
        }
        return jdbcTypeFormat;
    }

    @Override
    public JdbcTypeFormat getDefaultJdbcTypeFormat() {
        return defaultJdbcTypeFormat;
    }

    @Override
    public void setDefaultJdbcTypeFormat(JdbcTypeFormat defaultJdbcTypeFormat) {
        this.defaultJdbcTypeFormat = defaultJdbcTypeFormat;
    }

    @Override
    public JdbcTypeAccessor getJdbcTypeAccessor() {
        return jdbcTypeAccessor;
    }

    @Override
    public void setJdbcTypeAccessor(JdbcTypeAccessor jdbcTypeAccessor) {
        this.jdbcTypeAccessor = jdbcTypeAccessor;
    }

    @Override
    public ColumnSetModel getColumnSetModel() {
        return columnSetModel;
    }

    @Override
    public void setColumnSetModel(ColumnSetModel columnSetModel) {
        this.columnSetModel = columnSetModel;
    }
}
