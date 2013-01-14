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
import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

public class DriverConnectionProvider extends DataSourceConnectionProvider {

    private DriverConnectionSpec driverConnectionSpec;
    private Boolean autoCommit;
    private Integer transactionIsolation;

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

    @Override
    public String getCatalog() {
        return driverConnectionSpec != null ? driverConnectionSpec.getCatalog() : null;
    }

    @Override
    public String getSchema() {
        return driverConnectionSpec != null ? driverConnectionSpec.getSchema() : null;
    }

    protected DataSource createDataSource() throws SQLException {
        registerDriver();
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(driverConnectionSpec.getUrl());
        dataSource.setUsername(driverConnectionSpec.getUsername());
        dataSource.setPassword(driverConnectionSpec.getPassword());
        Map<String, String> properties = driverConnectionSpec.getProperties();
        if (properties != null) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                dataSource.addConnectionProperty(entry.getKey(), entry.getValue());
            }
        }
        return dataSource;
    }

    protected void registerDriver() throws SQLException {
        DriverConnectionSpec driverConnectionSpec = getDriverConnectionSpec();
        Driver driver = driverConnectionSpec.getDriver();
        if (driver == null) {
            String driverClassName = driverConnectionSpec.getDriverClassName();
            if (logger.isDebugEnabled()) {
                logger.debug(format("Loading driver %s", driverClassName));
            }
            driver = ReflectionUtils.newInstance(driverClassName);
        }
        DriverManager.registerDriver(driver);
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
        return driverConnectionSpec.getUrl();
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
}
