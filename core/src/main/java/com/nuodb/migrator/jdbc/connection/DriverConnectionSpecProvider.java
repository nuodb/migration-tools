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
package com.nuodb.migrator.jdbc.connection;

import com.nuodb.migrator.spec.DriverConnectionSpec;
import com.nuodb.migrator.utils.ReflectionUtils;
import org.apache.commons.dbcp.BasicDataSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

@SuppressWarnings("unchecked")
public class DriverConnectionSpecProvider extends ConnectionProxyProviderBase<DriverConnectionSpec> {

    private static final String GET_INNERMOST_DELEGATE = "getInnermostDelegate";

    private BasicDataSource basicDataSource;

    public DriverConnectionSpecProvider() {
    }

    public DriverConnectionSpecProvider(DriverConnectionSpec connectionSpec) {
        super(connectionSpec);
    }

    @Override
    protected Connection createTargetConnection() throws SQLException {
        if (basicDataSource == null) {
            DriverConnectionSpec driverConnectionSpec = getConnectionSpec();

            BasicDataSource basicDataSource = new BasicDataSource();
            basicDataSource.setDriverClassName(driverConnectionSpec.getDriverClassName());
            basicDataSource.setDriverClassLoader(ReflectionUtils.getClassLoader());
            basicDataSource.setUrl(driverConnectionSpec.getUrl());
            basicDataSource.setUsername(driverConnectionSpec.getUsername());
            basicDataSource.setPassword(driverConnectionSpec.getPassword());
            Map<String, Object> properties = driverConnectionSpec.getProperties();
            if (properties != null) {
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    basicDataSource.addConnectionProperty(entry.getKey(), (String) entry.getValue());
                }
            }
            basicDataSource.setAccessToUnderlyingConnectionAllowed(true);

            this.basicDataSource = basicDataSource;
        }
        return basicDataSource.getConnection();
    }

    @Override
    public Connection getTargetConnection(Connection connection) {
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
            Connection delegate = ReflectionUtils.invokeMethod(connection, method);
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
