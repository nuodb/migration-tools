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
package com.nuodb.migrator.spec;

import com.nuodb.migrator.jdbc.url.JdbcUrl;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.jdbc.url.JdbcUrlParsers.getInstance;

/**
 * @author Sergey Bushik
 */
public class DriverConnectionSpec extends ConnectionSpecBase {

    private String driver;
    private String url;
    private String username;
    private transient String password;
    private Map<String, Object> properties = newHashMap();

    @Override
    public String getCatalog() {
        String schema;
        JdbcUrl jdbcUrl;
        if ((schema = super.getCatalog()) == null && (jdbcUrl = getJdbcUrl()) != null) {
            schema = jdbcUrl.getCatalog();
        }
        return schema;
    }

    @Override
    public String getSchema() {
        String schema;
        JdbcUrl jdbcUrl;
        if ((schema = super.getSchema()) == null && (jdbcUrl = getJdbcUrl()) != null) {
            schema = jdbcUrl.getSchema();
        }
        return schema;
    }

    public JdbcUrl getJdbcUrl() {
        return getInstance().parse(url, properties);
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void addProperty(String property, Object value) {
        properties.put(property, value);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        DriverConnectionSpec that = (DriverConnectionSpec) o;

        if (driver != null ? !driver.equals(that.driver) : that.driver != null)
            return false;
        if (password != null ? !password.equals(that.password) : that.password != null)
            return false;
        if (properties != null ? !properties.equals(that.properties) : that.properties != null)
            return false;
        if (url != null ? !url.equals(that.url) : that.url != null)
            return false;
        if (username != null ? !username.equals(that.username) : that.username != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (driver != null ? driver.hashCode() : 0);
        result = 31 * result + (url != null ? url.hashCode() : 0);
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }
}
