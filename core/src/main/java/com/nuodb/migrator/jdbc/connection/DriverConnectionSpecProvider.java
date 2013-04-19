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

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

@SuppressWarnings("unchecked")
public class DriverConnectionSpecProvider extends ConnectionSpecProvider<DriverConnectionSpec> {

    private static final String GET_INNERMOST_DELEGATE = "getInnermostDelegate";

    private DataSource dataSource;
    private Boolean autoCommit;
    private Integer transactionIsolation;
    private boolean driverRegistered;

    public DriverConnectionSpecProvider(DriverConnectionSpec connectionSpec) {
        this(connectionSpec, false);
    }

    public DriverConnectionSpecProvider(DriverConnectionSpec connectionSpec, Boolean autoCommit) {
        this(connectionSpec, autoCommit, null);
    }

    public DriverConnectionSpecProvider(DriverConnectionSpec connectionSpec, Boolean autoCommit,
                                        Integer transactionIsolation) {
        super(connectionSpec);
        this.autoCommit = autoCommit;
        this.transactionIsolation = transactionIsolation;
    }

    protected Connection createConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    protected DataSource getDataSource() throws SQLException {
        if (dataSource == null) {
            dataSource = createDataSource();
        }
        return dataSource;
    }

    protected DataSource createDataSource() throws SQLException {
        if (!driverRegistered) {
            Driver driver = getConnectionSpec().getDriver();
            if (driver == null) {
                String driverClassName = getConnectionSpec().getDriverClassName();
                if (logger.isDebugEnabled()) {
                    logger.debug(format("Loading driver %s", driverClassName));
                }
                driver = ReflectionUtils.newInstance(driverClassName);
            }
            DriverManager.registerDriver(driver);
            driverRegistered = true;
        }
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(getConnectionSpec().getUrl());
        dataSource.setUsername(getConnectionSpec().getUsername());
        dataSource.setPassword(getConnectionSpec().getPassword());
        Map<String, Object> properties = getConnectionSpec().getProperties();
        if (properties != null) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                dataSource.addConnectionProperty(entry.getKey(), (String) entry.getValue());
            }
        }
        dataSource.setAccessToUnderlyingConnectionAllowed(true);
        return dataSource;
    }

    protected void initConnection(Connection connection) throws SQLException {
        if (autoCommit != null) {
            connection.setAutoCommit(autoCommit);
        }
        if (transactionIsolation != null) {
            connection.setTransactionIsolation(transactionIsolation);
        }
    }

    @Override
    public Connection unwrapConnection(Connection proxy) {
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

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        if (connection != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Closing connection");
            }
            connection.close();
        }
    }

    @Override
    public String toString() {
        return getConnectionSpec().getUrl();
    }

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public Integer getTransactionIsolation() {
        return transactionIsolation;
    }

    public void setTransactionIsolation(Integer transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }
}
