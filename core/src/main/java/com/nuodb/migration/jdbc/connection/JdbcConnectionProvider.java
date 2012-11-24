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

import com.nuodb.migration.jdbc.model.DatabaseInspector;
import com.nuodb.migration.spec.JdbcConnectionSpec;
import com.nuodb.migration.utils.ClassUtils;
import com.nuodb.migration.utils.ReflectionException;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcConnectionProvider implements ConnectionProvider {

    public static final String USER_PROPERTY = "user";
    public static final String PASSWORD_PROPERTY = "password";

    private transient final Log log = LogFactory.getLog(this.getClass());

    private JdbcConnectionSpec jdbcConnectionSpec;
    private Boolean autoCommit;
    private Integer transactionIsolation;
    private DataSource dataSource;

    public JdbcConnectionProvider(JdbcConnectionSpec jdbcConnectionSpec) {
        this(jdbcConnectionSpec, false);
    }

    public JdbcConnectionProvider(JdbcConnectionSpec jdbcConnectionSpec, boolean autoCommit) {
        this.jdbcConnectionSpec = jdbcConnectionSpec;
        this.autoCommit = autoCommit;
    }

    public JdbcConnectionProvider(JdbcConnectionSpec jdbcConnectionSpec, boolean autoCommit, int transactionIsolation) {
        this.jdbcConnectionSpec = jdbcConnectionSpec;
        this.autoCommit = autoCommit;
        this.transactionIsolation = transactionIsolation;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (dataSource == null) {
            dataSource = createDataSource();
        }
        return createConnection();
    }

    @Override
    public ConnectionServices getConnectionServices() throws SQLException {
        return new JdbcConnectionServices();
    }

    protected DataSource createDataSource() throws SQLException {
        try {
            Driver driver = jdbcConnectionSpec.getDriver();
            if (driver == null) {
                String driverClassName = jdbcConnectionSpec.getDriverClassName();
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Loading driver %s", driverClassName));
                }
                driver = ClassUtils.newInstance(driverClassName);
            }
            DriverManager.registerDriver(driver);
        } catch (ReflectionException exception) {
            if (log.isWarnEnabled()) {
                log.warn("Driver can't be loaded", exception);
            }
        }

        String url = jdbcConnectionSpec.getUrl();
        Properties properties = new Properties();
        if (jdbcConnectionSpec.getProperties() != null) {
            properties.putAll(jdbcConnectionSpec.getProperties());
        }
        String username = jdbcConnectionSpec.getUsername();
        String password = jdbcConnectionSpec.getPassword();
        if (username != null) {
            properties.setProperty(USER_PROPERTY, username);
        }
        if (password != null) {
            properties.setProperty(PASSWORD_PROPERTY, password);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Creating connection pool at %s", url));
        }
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
                new DriverManagerConnectionFactory(url, properties),
                new GenericObjectPool(null), null, null, false, true);
        return new PoolingDataSource(poolableConnectionFactory.getPool());
    }

    protected Connection createConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        if (autoCommit != null) {
            connection.setAutoCommit(autoCommit);
        }
        if (transactionIsolation != null) {
            connection.setTransactionIsolation(transactionIsolation);
        }
        return connection;
    }

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("Closing connection");
        }
        if (connection != null) {
            connection.close();
        }
    }

    class JdbcConnectionServices implements ConnectionServices {

        private Connection connection;

        @Override
        public Connection getConnection() throws SQLException {
            Connection current = connection;
            if (current == null) {
                synchronized (this) {
                    current = connection;
                    if (current == null) {
                        current = connection = JdbcConnectionProvider.this.getConnection();
                    }
                }
            }
            return current;
        }

        @Override
        public DatabaseInspector getDatabaseInspector() throws SQLException {
            DatabaseInspector databaseInspector = new DatabaseInspector();
            databaseInspector.withConnection(getConnection());
            databaseInspector.withCatalog(jdbcConnectionSpec.getCatalog());
            databaseInspector.withSchema(jdbcConnectionSpec.getSchema());
            return databaseInspector;
        }

        @Override
        public void close() throws SQLException {
            closeConnection(connection);
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
}
