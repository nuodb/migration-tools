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

import com.nuodb.migration.jdbc.model.ValueModel;
import com.nuodb.migration.jdbc.model.SimpleValueModel;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccess;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormat;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class SimpleValueFormatModel extends SimpleValueModel implements ValueFormatModel {

    private JdbcTypeValueFormat valueFormat;
    private JdbcTypeValueAccess valueAccess;
    private Map<String, Object> valueAccessOptions;

    public SimpleValueFormatModel(ValueModel valueModel, JdbcTypeValueFormat valueFormat,
                                  JdbcTypeValueAccess valueAccess,
                                  Map<String, Object> valueAccessOptions) {
        super(valueModel);
        this.valueFormat = valueFormat;
        this.valueAccess = valueAccess;
        this.valueAccessOptions = valueAccessOptions;
    }

    @Override
    public JdbcTypeValueFormat getValueFormat() {
        return valueFormat;
    }

    @Override
    public void setValueFormat(JdbcTypeValueFormat valueFormat) {
        this.valueFormat = valueFormat;
    }

    @Override
    public JdbcTypeValueAccess getValueAccess() {
        return valueAccess;
    }

    @Override
    public void setValueAccess(JdbcTypeValueAccess valueAccess) {
        this.valueAccess = valueAccess;
    }

    @Override
    public Map<String, Object> getValueAccessOptions() {
        return valueAccessOptions;
    }

    @Override
    public void setValueAccessOptions(Map<String, Object> valueAccessOptions) {
        this.valueAccessOptions = valueAccessOptions;
    }
}
