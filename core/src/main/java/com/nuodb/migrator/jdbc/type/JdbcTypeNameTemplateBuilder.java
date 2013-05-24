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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.quote;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeNameTemplateBuilder implements JdbcTypeNameBuilder {

    private final String template;

    public JdbcTypeNameTemplateBuilder(String template) {
        this.template = template;
    }

    @Override
    public String buildJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        return expandTemplate(getTemplate(jdbcTypeDesc, jdbcTypeSpecifiers), jdbcTypeSpecifiers);
    }

    protected String getTemplate(JdbcTypeDesc jdbcTypeDesc, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        return template;
    }

    protected String expandTemplate(String template, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        if (!isEmpty(template)) {
            template = expandPrecision(template, jdbcTypeSpecifiers.getPrecision());
            template = expandSize(template, jdbcTypeSpecifiers.getSize());
            template = expandScale(template, jdbcTypeSpecifiers.getScale());
        }
        return template;
    }

    protected String expandPrecision(String template, Integer precision) {
        return expandVariable(template, PRECISION, precision);
    }

    protected String expandSize(String template, Integer size) {
        return expandVariable(template, SIZE, size);
    }

    protected String expandScale(String template, Integer scale) {
        return expandVariable(template, SCALE, scale);
    }

    protected String expandVariable(String template, String variable, Integer value) {
        return expandVariable(template, variable, value != null ? Integer.toString(value) : null);
    }

    protected String expandVariable(String template, String variable, String value) {
        Pattern pattern = Pattern.compile(quote(VARIABLE_PREFIX + variable + VARIABLE_SUFFIX), CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(template);
        return value != null && matcher.find() ? matcher.replaceAll(value) : template;
    }
}
