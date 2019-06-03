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
package com.nuodb.migrator.jdbc.url;

import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.ORACLE_SUB_PROTOCOL;
import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.USER;
import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.PASSWORD;
import static org.apache.commons.lang3.StringUtils.substringBefore;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.nuodb.migrator.utils.StringUtils;

/**
 * @author Sergey Bushik
 */
public class OracleJdbcUrl extends JdbcUrlBase {

    private static final String ORACLE_JDBC_REG_EXPRESSION = "(.*?):(.*?):(.*?):((.*?)(\\/(.*?))?)?@(.*)";

    public static JdbcUrlParser getParser() {
        return new JdbcUrlParserBase(ORACLE_SUB_PROTOCOL) {
            @Override
            protected JdbcUrl createJdbcUrl(String url) {
                return new OracleJdbcUrl(url);
            }
        };
    }

    private String qualifier;

    protected OracleJdbcUrl(String url) {
        super(url, ORACLE_SUB_PROTOCOL);
    }

    @Override
    protected void parseSubName(String subName) {
        qualifier = substringBefore(subName, ":");
    }

    @Override
    protected void addParameters() {
        Pattern jdbcPattern = Pattern.compile(ORACLE_JDBC_REG_EXPRESSION);
        Matcher jdbcMatcher = jdbcPattern.matcher(getUrl());
        if (jdbcMatcher.matches()) {
            if (!StringUtils.isEmpty(jdbcMatcher.group(5)))
                addParameter(USER, jdbcMatcher.group(5));
            if (!StringUtils.isEmpty(jdbcMatcher.group(7)))
                addParameter(PASSWORD, jdbcMatcher.group(7));
        }
    }

    public String getQualifier() {
        return qualifier;
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public String getSchema() {
        return null;
    }
}