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

import com.nuodb.tools.migration.definition.JdbcConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcConnectionProviderImpl implements JdbcConnectionProvider {

    public static final String USER_PROPERTY = "user";
    public static final String PASSWORD_PROPERTY = "password";

    private transient final Log log = LogFactory.getLog(this.getClass());

    private JdbcConnection connection;

    public JdbcConnectionProviderImpl(JdbcConnection connection) {
        this.connection = connection;
    }

    public java.sql.Connection getConnection() throws SQLException {
        loadDriver();
        return createConnection();
    }

    protected void loadDriver() {
        try {
            String driver = connection.getDriver();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Loading driver %s", driver));
            }
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            if (log.isWarnEnabled()) {
                log.warn("Driver can't be loaded", e);
            }
        }
    }

    protected java.sql.Connection createConnection() throws SQLException {
        String url = connection.getUrl();
        Properties properties = new Properties();
        if (connection.getProperties() != null) {
            properties.putAll(connection.getProperties());
        }
        String username = connection.getUsername();
        String password = connection.getPassword();
        if (username != null) {
            properties.setProperty(USER_PROPERTY, username);
        }
        if (password != null) {
            properties.setProperty(PASSWORD_PROPERTY, password);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Creating new connection at %s", url));
        }
        return DriverManager.getConnection(url, properties);
    }

    public void closeConnection(java.sql.Connection connection) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("Closing connection");
        }
        connection.close();
    }
}
