/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import com.google.common.collect.Lists;

import java.util.Collection;

import static com.google.common.collect.Sets.newLinkedHashSet;

/**
 * @author Sergey Bushik
 */
public class JdbcEnumType extends JdbcType {

    private Collection<String> values = newLinkedHashSet();

    public JdbcEnumType() {
    }

    public JdbcEnumType(JdbcTypeDesc jdbcTypeDesc) {
        super(jdbcTypeDesc);
    }

    public JdbcEnumType(JdbcTypeOptions jdbcTypeOptions) {
        super(jdbcTypeOptions);
    }

    public JdbcEnumType(JdbcTypeDesc jdbcTypeDesc, JdbcTypeOptions jdbcTypeOptions) {
        super(jdbcTypeDesc, jdbcTypeOptions);
    }

    public JdbcEnumType(JdbcType jdbcType, Collection<String> values) {
        super(jdbcType);
        this.values = values;
    }

    public void addValue(String value) {
        values.add(value);
    }

    public Collection<String> getValues() {
        return values;
    }

    public void setValues(Collection<String> values) {
        this.values = values;
    }

    @Override
    protected JdbcType createJdbcType() {
        JdbcEnumType jdbcType = (JdbcEnumType) super.createJdbcType();
        jdbcType.setValues(Lists.newArrayList(getValues()));
        return jdbcType;
    }
}
