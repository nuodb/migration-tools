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

import com.nuodb.migrator.spec.JdbcConnectionSpec;
import com.nuodb.migrator.utils.ReflectionUtils;
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
public class JdbcPoolingConnectionProvider extends JdbcConnectionProvider {

    private static final String GET_INNERMOST_DELEGATE = "getInnermostDelegate";
    public static final String USER_PROPERTY = "user";
    public static final String PASSWORD_PROPERTY = "password";

    public JdbcPoolingConnectionProvider(JdbcConnectionSpec connectionSpec) {
        super(connectionSpec);
    }

    public JdbcPoolingConnectionProvider(JdbcConnectionSpec connectionSpec, boolean autoCommit) {
        super(connectionSpec, autoCommit);
    }

    public JdbcPoolingConnectionProvider(JdbcConnectionSpec connectionSpec, boolean autoCommit,
                                         int transactionIsolation) {
        super(connectionSpec, autoCommit, transactionIsolation);
    }

    @Override
    protected PoolingDataSource createDataSource() throws SQLException {
        registerDriver();

        JdbcConnectionSpec connectionSpec = getJdbcConnectionSpec();
        String url = connectionSpec.getUrl();
        Properties properties = new Properties();
        if (connectionSpec.getProperties() != null) {
            properties.putAll(connectionSpec.getProperties());
        }
        String username = connectionSpec.getUsername();
        String password = connectionSpec.getPassword();
        if (username != null) {
            properties.setProperty(USER_PROPERTY, username);
        }
        if (password != null) {
            properties.setProperty(PASSWORD_PROPERTY, password);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Creating connection pool at %s", url));
        }
        PoolableConnectionFactory connectionFactory = new PoolableConnectionFactory(
                new DriverManagerConnectionFactory(url, properties),
                new GenericObjectPool(null), null, null, false, true);
        PoolingDataSource dataSource = new PoolingDataSource(connectionFactory.getPool());
        dataSource.setAccessToUnderlyingConnectionAllowed(true);
        return dataSource;
    }

    @Override
    protected Connection invokeGetConnection(Connection proxy) {
        if (proxy == null) {
            return null;
        }
        try {
            Class connectionClass = proxy.getClass();
            while (!Modifier.isPublic(connectionClass.getModifiers())) {
                connectionClass = connectionClass.getSuperclass();
                if (connectionClass == null) {
                    return proxy;
                }
            }
            Method method = connectionClass.getMethod(GET_INNERMOST_DELEGATE, (Class[]) null);
            Connection delegate = ReflectionUtils.invokeMethod(proxy, method);
            return (delegate != null ? delegate : proxy);
        } catch (NoSuchMethodException exception) {
            return proxy;
        }
    }
}
