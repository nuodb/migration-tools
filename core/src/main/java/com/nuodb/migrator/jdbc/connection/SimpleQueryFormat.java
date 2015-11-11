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
package com.nuodb.migrator.jdbc.connection;

import java.sql.Blob;
import java.sql.Clob;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.valueOf;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;
import static org.apache.commons.lang3.StringUtils.countMatches;

/**
 * @author Sergey Bushik
 */
public class SimpleQueryFormat implements QueryFormat {

    private static final String PARAMETER = "?";
    public static final Pattern PARAMETER_PATTERN = compile(quote(PARAMETER));

    private final String query;
    private final List<Object> parameters;

    public SimpleQueryFormat(String query) {
        this.query = query;
        this.parameters = newArrayList(new Object[countMatches(query, PARAMETER)]);
    }

    public String format() {
        Matcher matcher = PARAMETER_PATTERN.matcher(query);
        StringBuffer result = new StringBuffer(query.length());
        int index = 0;
        while (matcher.find()) {
            matcher.appendReplacement(result, quoteReplacement(format(getParameter(index++))));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    protected String format(Object parameter) {
        if (parameter == null) {
            return "NULL";
        }
        if (parameter instanceof Clob) {
            return "[CLOB]";
        } else if (parameter instanceof Blob) {
            return "[BLOB]";
        } else if (parameter instanceof Number) {
            return valueOf(parameter);
        } else {
            return "'" + valueOf(parameter) + "'";
        }
    }

    public Object getParameter(int index) {
        return parameters.get(index);
    }

    public void setParameter(int index, Object value) {
        parameters.set(index, value);
    }
}