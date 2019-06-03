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

import com.nuodb.migrator.utils.ObjectUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.quote;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeNameTemplate extends HasJdbcTypeHandlerBase implements JdbcTypeName {

    public static final String SIZE = "N";
    public static final String PRECISION = "P";
    public static final String SCALE = "S";
    public static final String VARIABLE_PREFIX = "{";
    public static final String VARIABLE_SUFFIX = "}";

    private String template;

    public JdbcTypeNameTemplate(String template) {
        this.template = template;
    }

    public JdbcTypeNameTemplate(JdbcType jdbcType, String template) {
        super(jdbcType);
        this.template = template;
    }

    @Override
    public String getTypeName(JdbcType jdbcType) {
        String template = getTemplate();
        if (!isEmpty(template)) {
            JdbcTypeOptions jdbcTypeOptions = jdbcType.getJdbcTypeOptions();
            template = expandPrecision(template, jdbcTypeOptions.getPrecision());
            template = expandSize(template, jdbcTypeOptions.getSize());
            template = expandScale(template, jdbcTypeOptions.getScale());
        }
        return template;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    protected String expandPrecision(String template, Integer precision) {
        return expandVariable(template, PRECISION, precision);
    }

    protected String expandSize(String template, Long size) {
        return expandVariable(template, SIZE, size);
    }

    protected String expandScale(String template, Integer scale) {
        return expandVariable(template, SCALE, scale);
    }

    protected String expandVariable(String template, String variable, Integer value) {
        return expandVariable(template, variable, value != null ? Integer.toString(value) : null);
    }

    protected String expandVariable(String template, String variable, Long value) {
        return expandVariable(template, variable, value != null ? Long.toString(value) : null);
    }

    protected String expandVariable(String template, String variable, String value) {
        Pattern pattern = Pattern.compile(quote(VARIABLE_PREFIX + variable + VARIABLE_SUFFIX), CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(template);
        return value != null && matcher.find() ? matcher.replaceAll(value) : template;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        JdbcTypeNameTemplate that = (JdbcTypeNameTemplate) o;

        if (template != null ? !template.equals(that.template) : that.template != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (template != null ? template.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
