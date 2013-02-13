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
package com.nuodb.migration.resultset.format;

import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.model.ValueModelList;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessProvider;
import com.nuodb.migration.jdbc.type.jdbc2.JdbcDateTypeBase;
import com.nuodb.migration.resultset.format.value.ValueFormatModel;
import com.nuodb.migration.resultset.format.value.ValueFormatRegistry;

import java.sql.Types;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author Sergey Bushik
 */
public abstract class ResultSetFormatBase implements ResultSetFormat {

    private TimeZone timeZone;
    private Map<String, Object> attributes;
    private ValueModelList<ValueFormatModel> valueFormatModelList;
    private ValueFormatRegistry valueFormatRegistry;
    private JdbcTypeValueAccessProvider valueAccessProvider;

    @Override
    public Object getAttribute(String attribute) {
        return attributes != null ? attributes.get(attribute) : null;
    }

    @Override
    public Object getAttribute(String attribute, Object defaultValue) {
        Object value = null;
        if (attributes != null) {
            value = attributes.get(attribute);
        }
        return value == null ? defaultValue : value;
    }

    @Override
    public final void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    protected ValueFormatModel visitValueFormatModel(ValueFormatModel valueFormatModel) {
        int typeCode = valueFormatModel.getTypeCode();
        if (typeCode == Types.TIME || typeCode == Types.TIMESTAMP || typeCode == Types.DATE) {
            TimeZone timeZone = getTimeZone();
            if (timeZone != null) {
                Map<String, Object> valueAccessOptions = Maps.newHashMap();
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(timeZone);
                valueAccessOptions.put(JdbcDateTypeBase.CALENDAR, calendar);
                valueFormatModel.setValueAccessOptions(valueAccessOptions);
            }
        }
        return valueFormatModel;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public TimeZone getTimeZone() {
        return timeZone;
    }

    @Override
    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public ValueModelList<ValueFormatModel> getValueFormatModelList() {
        return valueFormatModelList;
    }

    @Override
    public void setValueFormatModelList(ValueModelList<ValueFormatModel> valueFormatModelList) {
        this.valueFormatModelList = valueFormatModelList;
    }

    @Override
    public JdbcTypeValueAccessProvider getValueAccessProvider() {
        return valueAccessProvider;
    }

    @Override
    public void setValueAccessProvider(JdbcTypeValueAccessProvider valueAccessProvider) {
        this.valueAccessProvider = valueAccessProvider;
    }

    @Override
    public ValueFormatRegistry getValueFormatRegistry() {
        return valueFormatRegistry;
    }

    @Override
    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        this.valueFormatRegistry = valueFormatRegistry;
    }
}
