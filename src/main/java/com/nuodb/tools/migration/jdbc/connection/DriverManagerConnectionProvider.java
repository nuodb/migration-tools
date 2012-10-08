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
package com.nuodb.tools.migration.jdbc.connection;

import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerConnectionProvider implements ConnectionProvider {

    public static final String USER_PROPERTY = "user";
    public static final String PASSWORD_PROPERTY = "password";

    private transient final Log log = LogFactory.getLog(this.getClass());
    
    private DriverManagerConnectionSpec driverManagerConnectionSpec;
    private Boolean autoCommit = Boolean.FALSE;
    private Integer transactionIsolation;

    public DriverManagerConnectionProvider(DriverManagerConnectionSpec driverManagerConnectionSpec) {
        this.driverManagerConnectionSpec = driverManagerConnectionSpec;
    }

    public DriverManagerConnectionProvider(DriverManagerConnectionSpec driverManagerConnectionSpec, Boolean autoCommit) {
        this.driverManagerConnectionSpec = driverManagerConnectionSpec;
        this.autoCommit = autoCommit;
    }

    public DriverManagerConnectionProvider(DriverManagerConnectionSpec driverManagerConnectionSpec, Boolean autoCommit, Integer transactionIsolation) {
        this.driverManagerConnectionSpec = driverManagerConnectionSpec;
        this.autoCommit = autoCommit;
        this.transactionIsolation = transactionIsolation;
    }

    public Connection getConnection() throws SQLException {
        try {
            String driver = driverManagerConnectionSpec.getDriver();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Loading driver %s", driver));
            }
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            if (log.isWarnEnabled()) {
                log.warn("Driver can't be loaded", e);
            }
        }
        return createConnection();
    }

    protected Connection createConnection() throws SQLException {
        String url = driverManagerConnectionSpec.getUrl();
        Properties properties = new Properties();
        if (driverManagerConnectionSpec.getProperties() != null) {
            properties.putAll(driverManagerConnectionSpec.getProperties());
        }
        String username = driverManagerConnectionSpec.getUsername();
        String password = driverManagerConnectionSpec.getPassword();
        if (username != null) {
            properties.setProperty(USER_PROPERTY, username);
        }
        if (password != null) {
            properties.setProperty(PASSWORD_PROPERTY, password);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Creating new connection at %s", url));
        }
        Connection connection = DriverManager.getConnection(url, properties);
        if (transactionIsolation != null) {
            connection.setTransactionIsolation(transactionIsolation);
        }
        if (autoCommit != null) {
            connection.setAutoCommit(autoCommit);
        }
        return connection;
    }

    public void closeConnection(Connection connection) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("Closing connection");
        }
        if (connection != null) {
            connection.close();
        }
    }

    public Integer getTransactionIsolation() {
        return transactionIsolation;
    }

    public void setTransactionIsolation(Integer transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }
}
