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

import java.util.Map;

/**
 * @author Sergey Bushik
 */
public interface JdbcUrl {

    /**
     * Returns full url jdbc:com.nuodb://localhost/test
     *
     * @return original url
     */
    String getUrl();

    /**
     * Always returns "jdbc"
     *
     * @return constant protocol part of the url, which is "jdbc"
     */
    String getProtocol();

    /**
     * Vendor dependent sub protocol name, i.e. for
     * jdbc:com.nuodb://localhost/test returns "com.nuodb"
     *
     * @return sub protocol part of the url
     */
    String getSubProtocol();

    /**
     * Vendor dependent qualifier of the data source, i.e. for
     * jdbc:jtds:sqlserver://localhost:1433/test sub protocol is "jtds" and
     * qualifier is "sqlserver"
     *
     * @return qualifier describing data source.
     */
    String getQualifier();

    /**
     * Default catalog to be used by driver
     *
     * @return the name of the default catalog
     */
    String getCatalog();

    /**
     * Default schema to be used by driver
     *
     * @return the name of the default schema
     */
    String getSchema();

    /**
     * Adds parameter with specified value to the url parameters.
     *
     * @param parameter
     *            name of the parameter to add.
     * @param value
     *            parameter value.
     */
    void addParameter(String parameter, Object value);

    /**
     * Merges parameters with current url parameters.
     *
     * @param parameters
     *            to be added to url parameters.
     */
    void addParameters(Map<String, Object> parameters);

    /**
     * Optional key value parameters
     *
     * @return
     */
    Map<String, Object> getParameters();
}
