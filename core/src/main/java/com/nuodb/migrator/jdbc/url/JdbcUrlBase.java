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

import com.nuodb.migrator.utils.ObjectUtils;

import java.util.HashMap;
import java.util.Map;

import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.PROTOCOL;
import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.SEPARATOR;
import static org.apache.commons.lang3.StringUtils.split;
import static org.apache.commons.lang3.StringUtils.substring;

/**
 * @author Sergey Bushik
 */
public abstract class JdbcUrlBase implements JdbcUrl {

    private final String url;
    private final String subProtocol;
    private Map<String, Object> parameters = new HashMap<String, Object>();

    protected JdbcUrlBase(String url, String subProtocol) {
        this.url = url;
        this.subProtocol = subProtocol;
        addParameters();
        parseUrl();
    }

    protected void addParameters() {
    }

    protected void parseUrl() {
        parseSubName(substring(url, (PROTOCOL + SEPARATOR + subProtocol + SEPARATOR).length()));
    }

    protected void parseSubName(String subName) {
    }

    public static void parseParameters(Map<String, Object> parameters, String url, String separator) {
        String[] pairs = split(url, separator);
        for (String pair : pairs) {
            String[] values = pair.split("=");
            parameters.put(values[0], values.length > 1 ? values[1] : null);
        }
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public String getProtocol() {
        return PROTOCOL;
    }

    @Override
    public String getSubProtocol() {
        return subProtocol;
    }

    @Override
    public void addParameter(String key, Object value) {
        parameters.put(key, value);
    }

    @Override
    public void addParameters(Map<String, Object> parameters) {
        this.parameters.putAll(parameters);
    }

    @Override
    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}