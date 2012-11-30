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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverConnectionProvider implements ConnectionProvider {

    public static final String USER_PROPERTY = "user";
    public static final String PASSWORD_PROPERTY = "password";

    protected transient final Logger logger = LoggerFactory.getLogger(this.getClass());

    private DriverConnectionSpec driverConnectionSpec;
    private Boolean autoCommit;
    private Integer transactionIsolation;
    private boolean initDriver;

    public DriverConnectionProvider(DriverConnectionSpec driverConnectionSpec) {
        this(driverConnectionSpec, false);
    }

    public DriverConnectionProvider(DriverConnectionSpec driverConnectionSpec, boolean autoCommit) {
        this.driverConnectionSpec = driverConnectionSpec;
        this.autoCommit = autoCommit;
    }

    public DriverConnectionProvider(DriverConnectionSpec driverConnectionSpec, boolean autoCommit,
                                    int transactionIsolation) {
        this.driverConnectionSpec = driverConnectionSpec;
        this.autoCommit = autoCommit;
        this.transactionIsolation = transactionIsolation;
    }

    protected DriverConnectionSpec getDriverConnectionSpec() {
        return driverConnectionSpec;
    }

    protected String getCatalog() {
        return driverConnectionSpec != null ? driverConnectionSpec.getCatalog() : null;
    }

    protected String getSchema() {
        return driverConnectionSpec != null ? driverConnectionSpec.getSchema() : null;
    }

    @Override
    public ConnectionServices getConnectionServices() {
        return new DriverConnectionServices(this);
    }

    protected void initDriver() throws SQLException {
        if (!initDriver) {
            synchronized (this) {
                if (!initDriver) {
                    loadDriver();
                    initDriver = true;
                }
            }
        }
    }

    protected void loadDriver() throws SQLException {
        DriverConnectionSpec driverConnectionSpec = getDriverConnectionSpec();
        Driver driver = driverConnectionSpec.getDriver();
        if (driver == null) {
            String driverClassName = driverConnectionSpec.getDriverClassName();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Loading driver %s", driverClassName));
            }
            driver = ReflectionUtils.newInstance(driverClassName);
        }
        DriverManager.registerDriver(driver);
    }

    @Override
    public Connection getConnection() throws SQLException {
        initDriver();
        Connection connection = createConnection();
        initConnection(connection);
        return createConnectionProxy(connection);
    }

    protected void initConnection(Connection connection) throws SQLException {
        if (autoCommit != null) {
            connection.setAutoCommit(autoCommit);
        }
        if (transactionIsolation != null) {
            connection.setTransactionIsolation(transactionIsolation);
        }
    }

    protected ConnectionProxy createConnectionProxy(Connection connection) {
        return (ConnectionProxy) Proxy.newProxyInstance(ReflectionUtils.getClassLoader(),
                new Class<?>[]{ConnectionProxy.class}, new ConnectionProxyHandler(connection));
    }

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        if (connection != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Closing connection");
            }
            connection.close();
        }
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public int getTransactionIsolation() {
        return transactionIsolation;
    }

    public void setTransactionIsolation(int transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }

    class ConnectionProxyHandler implements InvocationHandler {

        private static final String GET_CONNECTION = "getConnection";
        private Connection connection;

        public ConnectionProxyHandler(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (GET_CONNECTION.equals(methodName)) {
                return getConnection(connection);
            }
            return ReflectionUtils.invokeExactMethod(connection, methodName, args);
        }
    }

    protected Connection createConnection() throws SQLException {
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
        return DriverManager.getConnection(url, properties);
    }

    protected Connection getConnection(Connection connection) {
        return connection;
    }
}
