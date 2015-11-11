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

import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.MYSQL_SUB_PROTOCOL;
import static org.apache.commons.lang3.StringUtils.substring;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;

/**
 * @author Sergey Bushik
 */
public class MySQLJdbcUrl extends JdbcUrlBase {

    public static final String ZERO_DATE_TIME_BEHAVIOR = "zeroDateTimeBehavior";
    public static final String CONVERT_TO_NULL = "convertToNull";
    public static final String ROUND = "round";
    public static final String EXCEPTION = "exception";
    public static final String DEFAULT_BEHAVIOR = CONVERT_TO_NULL;

    public static JdbcUrlParser getParser() {
        return new JdbcUrlParserBase(MYSQL_SUB_PROTOCOL) {
            @Override
            protected JdbcUrl createJdbcUrl(String url) {
                return new MySQLJdbcUrl(url);
            }
        };
    }

    private String catalog;

    protected MySQLJdbcUrl(String url) {
        super(url, MYSQL_SUB_PROTOCOL);
    }

    @Override
    protected void addParameters() {
        addParameter(ZERO_DATE_TIME_BEHAVIOR, DEFAULT_BEHAVIOR);
    }

    @Override
    protected void parseSubName(String subName) {
        int prefix = subName.indexOf("//");
        int parameters = 0;
        if (prefix >= 0 && (parameters = subName.indexOf('?', prefix + 3)) > 0) {
            parseParameters(getParameters(), substring(subName, parameters + 1), "&");
        }
        String base = parameters > 0 ? subName.substring(0, parameters) : subName;
        catalog = substringAfterLast(base, "/");
    }

    @Override
    public String getCatalog() {
        return catalog;
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public String getQualifier() {
        return null;
    }
}