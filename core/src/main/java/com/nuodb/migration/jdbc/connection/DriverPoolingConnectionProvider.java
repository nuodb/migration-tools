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
package com.nuodb.migration.jdbc.connection;

import com.nuodb.migration.spec.DriverConnectionSpec;
import com.nuodb.migration.utils.ReflectionUtils;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Uses commons DBCP for pooling connections.
 *
 * @author Sergey Bushik
 */
public class DriverPoolingConnectionProvider extends DriverConnectionProvider {

    private static final String GET_INNERMOST_DELEGATE = "getInnermostDelegate";

    private PoolingDataSource dataSource;

    public DriverPoolingConnectionProvider(DriverConnectionSpec driverConnectionSpec) {
        super(driverConnectionSpec);
    }

    public DriverPoolingConnectionProvider(DriverConnectionSpec driverConnectionSpec, boolean autoCommit) {
        super(driverConnectionSpec, autoCommit);
    }

    public DriverPoolingConnectionProvider(DriverConnectionSpec driverConnectionSpec, boolean autoCommit,
                                           int transactionIsolation) {
        super(driverConnectionSpec, autoCommit, transactionIsolation);
    }

    @Override
    protected Connection createConnection() throws SQLException {
        initDataSource();
        return dataSource.getConnection();
    }

    protected void initDataSource() throws SQLException {
        if (dataSource == null) {
            synchronized (this) {
                if (dataSource == null) {
                    dataSource = createDataSource();
                }
            }
            dataSource.setAccessToUnderlyingConnectionAllowed(true);
        }
    }

    protected PoolingDataSource createDataSource() throws SQLException {
        DriverConnectionSpec driverConnectionSpec = getDriverConnectionSpec();
        String url = driverConnectionSpec.getUrl();
        Properties properties = new Properties();
        if (driverConnectionSpec.getProperties() != null) {
            properties.putAll(driverConnectionSpec.getProperties());
        }
        String username = driverConnectionSpec.getUsername();
        String password = driverConnectionSpec.getPassword();
        if (username != null) {
            properties.setProperty(USER_PROPERTY, username);
        }
        if (password != null) {
            properties.setProperty(PASSWORD_PROPERTY, password);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Creating connection pool at %s", url));
        }
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
                new DriverManagerConnectionFactory(url, properties),
                new GenericObjectPool(null), null, null, false, true);
        return new PoolingDataSource(poolableConnectionFactory.getPool());
    }

    @Override
    protected Connection getConnection(Connection connection) {
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
            Connection targetConnection = ReflectionUtils.invokeMethod(connection, method);
            return (targetConnection != null ? targetConnection : connection);
        } catch (NoSuchMethodException exception) {
            return connection;
        }
    }
}
