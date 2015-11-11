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

import com.nuodb.migrator.jdbc.url.JdbcUrl;
import com.nuodb.migrator.spec.DriverConnectionSpec;
import org.apache.commons.dbcp.BasicDataSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static com.nuodb.migrator.utils.Collections.isEmpty;
import static com.nuodb.migrator.utils.ReflectionUtils.getClassLoader;
import static com.nuodb.migrator.utils.ReflectionUtils.invokeMethod;

@SuppressWarnings("unchecked")
public class DriverConnectionProvider extends ConnectionProxyProviderBase<DriverConnectionSpec> {

    private static final String GET_INNERMOST_DELEGATE = "getInnermostDelegate";

    private BasicDataSource basicDataSource;

    public DriverConnectionProvider(DriverConnectionSpec connectionSpec) {
        super(connectionSpec);
    }

    @Override
    protected Connection createConnection() throws SQLException {
        synchronized (this) {
            if (basicDataSource == null) {
                DriverConnectionSpec connectionSpec = getConnectionSpec();

                BasicDataSource basicDataSource = new BasicDataSource();
                basicDataSource.setDriverClassName(connectionSpec.getDriver());
                basicDataSource.setDriverClassLoader(getClassLoader());
                basicDataSource.setUrl(connectionSpec.getUrl());
                basicDataSource.setUsername(connectionSpec.getUsername());
                basicDataSource.setPassword(connectionSpec.getPassword());
                JdbcUrl jdbcUrl = connectionSpec.getJdbcUrl();
                if (jdbcUrl != null) {
                    addParameters(basicDataSource, jdbcUrl.getParameters());
                }
                addParameters(basicDataSource, connectionSpec.getProperties());
                basicDataSource.setAccessToUnderlyingConnectionAllowed(true);

                this.basicDataSource = basicDataSource;
            }
        }
        return basicDataSource.getConnection();
    }

    protected void addParameters(BasicDataSource basicDataSource, Map<String, Object> properties) {
        if (!isEmpty(properties)) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                basicDataSource.addConnectionProperty(entry.getKey(), (String) entry.getValue());
            }
        }
    }

    @Override
    public Connection getConnection(Connection connection) {
        if (connection == null) {
            return null;
        }
        try {
            Class connectionClass = connection.getClass();
            while (!Modifier.isPublic(connectionClass.getModifiers())) {
                connectionClass = connectionClass.getSuperclass();
                if (connectionClass == null) {
                    return connection;
                }
            }
            Method method = connectionClass.getMethod(GET_INNERMOST_DELEGATE, (Class[]) null);
            Connection delegate = invokeMethod(connection, method);
            return (delegate != null ? delegate : connection);
        } catch (NoSuchMethodException exception) {
            return connection;
        }
    }

    @Override
    public void close() throws SQLException {
        if (basicDataSource != null) {
            basicDataSource.close();
        }
    }
}
