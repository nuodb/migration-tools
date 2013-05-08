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

import java.lang.reflect.Method;
import java.sql.*;
import java.util.Collection;

import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.utils.ReflectionUtils.getClassLoader;
import static java.lang.reflect.Proxy.newProxyInstance;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class StatementLoggingConnectionProvider extends ConnectionProxyProvider {

    private final ConnectionProvider connectionProvider;
    private final StatementFormatterFactory statementFormatterFactory;
    private final StatementLogger statementLogger;

    public StatementLoggingConnectionProvider(ConnectionProvider connectionProvider) {
        this(connectionProvider, new SimpleStatementFormatterFactory());
    }

    public StatementLoggingConnectionProvider(ConnectionProvider connectionProvider,
                                              StatementFormatterFactory statementFormatterFactory) {
        this(connectionProvider, statementFormatterFactory,
                new SimpleStatementLogger(getLogger(StatementLoggingConnectionProvider.class)));
    }

    public StatementLoggingConnectionProvider(ConnectionProvider connectionProvider,
                                              StatementFormatterFactory statementFormatterFactory,
                                              StatementLogger statementLogger) {
        this.connectionProvider = connectionProvider;
        this.statementFormatterFactory = statementFormatterFactory;
        this.statementLogger = statementLogger;
    }

    @Override
    public String getCatalog() {
        return connectionProvider.getCatalog();
    }

    @Override
    public String getSchema() {
        return connectionProvider.getSchema();
    }

    @Override
    protected Connection createConnection() throws SQLException {
        if (connectionProvider instanceof ConnectionProxyProvider) {
            return ((ConnectionProxyProvider) connectionProvider).createConnection();
        } else {
            return connectionProvider.getConnection();
        }
    }

    @Override
    protected void initConnection(Connection connection) throws SQLException {
        if (connectionProvider instanceof ConnectionProxyProvider) {
            ((ConnectionProxyProvider) connectionProvider).initConnection(connection);
        }
    }

    @Override
    protected Connection wrapConnection(Connection connection) {
        return (Connection) newProxyInstance(getClassLoader(),
                new Class<?>[]{ConnectionProxy.class}, new ConnectionHandler(connection));
    }

    @Override
    public Connection unwrapConnection(Connection connection) {
        if (connectionProvider instanceof ConnectionProxyProvider) {
            return ((ConnectionProxyProvider) connectionProvider).unwrapConnection(connection);
        } else {
            return connection;
        }
    }

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        connectionProvider.closeConnection(connection);
    }

    protected StatementFormatter createStatementFormatter(Statement statement, String query) {
        return statementFormatterFactory.createStatementFormat(statement, query);
    }

    protected void log(Statement statement, String query) {
        log(createStatementFormatter(statement, query).format());
    }

    protected void log(String statement) {
        statementLogger.log(statement);
    }

    protected Object invokeGetMetaData(TargetHandler<Connection> targetHandler,
                                       Object proxy, Method method, Object[] args) throws Throwable {
        DatabaseMetaData databaseMetaData = targetHandler.invokeTarget(method, args);
        return newProxyInstance(getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                new ConnectionAwareHandler((Connection) proxy, databaseMetaData));
    }

    protected Object invokeCreateStatement(TargetHandler<Connection> targetHandler,
                                           Object proxy, Method method, Object[] args) throws Throwable {
        Statement statement = targetHandler.invokeTarget(method, args);
        return newProxyInstance(getClassLoader(), new Class<?>[]{Statement.class},
                new StatementHandler((Connection) proxy, statement));
    }

    protected Object invokePrepareStatement(TargetHandler<Connection> targetHandler,
                                            Object proxy, Method method, Object[] args) throws Throwable {
        PreparedStatement preparedStatement = targetHandler.invokeTarget(method, args);
        return newProxyInstance(getClassLoader(), new Class<?>[]{PreparedStatement.class},
                new PreparedStatementHandler((Connection) proxy, preparedStatement,
                        (String) args[0]));
    }

    protected Object invokeExecute(TargetHandler<? extends Statement> targetHandler,
                                   Object proxy, Method method, Object[] args) throws Throwable {
        return ((StatementHandler) targetHandler).invokeExecute(method, args);
    }

    protected Object invokeSet(TargetHandler<PreparedStatement> targetHandler,
                               Object proxy, Method method, Object[] args) throws Throwable {
        return ((PreparedStatementHandler) targetHandler).invokeSet(method, args);
    }

    protected class ConnectionHandler extends ConnectionProxyProvider.ConnectionHandler {

        private static final String GET_META_DATA_METHOD = "getMetaData";

        private static final String CREATE_STATEMENT_METHOD = "createStatement";

        private static final String PREPARE_STATEMENT_METHOD = "prepareStatement";

        public ConnectionHandler(Connection connection) {
            super(connection);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (CREATE_STATEMENT_METHOD.equals(method.getName())) {
                return invokeCreateStatement(this, proxy, method, args);
            } else if (PREPARE_STATEMENT_METHOD.equals(method.getName()) && args != null) {
                return invokePrepareStatement(this, proxy, method, args);
            } else if (GET_META_DATA_METHOD.equals(method.getName())) {
                return invokeGetMetaData(this, proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }
    }

    protected class ConnectionAwareHandler<T> extends TargetHandler<T> {

        private static final String GET_CONNECTION_METHOD = "getConnection";

        private Connection connection;

        public ConnectionAwareHandler(Connection connection, T target) {
            super(target);
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (GET_CONNECTION_METHOD.equals(method.getName())) {
                return connection;
            } else {
                return super.invoke(proxy, method, args);
            }
        }
    }

    public class StatementHandler<T extends Statement> extends ConnectionAwareHandler<T> {

        private final Collection<String> EXECUTE_METHODS = newHashSet("execute", "executeQuery", "executeUpdate");

        public StatementHandler(Connection connection, T statement) {
            super(connection, statement);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (EXECUTE_METHODS.contains(methodName)) {
                return StatementLoggingConnectionProvider.this.invokeExecute(this, proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }

        public Object invokeExecute(Method method, Object[] args) throws Throwable {
            log(getTarget(), (String) args[0]);
            return invokeTarget(method, args);
        }
    }

    protected class PreparedStatementHandler extends StatementHandler<PreparedStatement> {

        private static final String SET_NULL = "setNull";

        private final Collection<String> SET_METHODS = newHashSet(
                "setNull", "setBoolean", "setByte", "setShort", "setInt", "setLong", "setFloat", "setDouble",
                "setBigDecimal", "setString", "setBytes", "setDate", "setTime", "setTimestamp", "setAsciiStream",
                "setUnicodeStream", "setBinaryStream", "setObject", "setCharacterStream", "setRef", "setBlob",
                "setClob", "setArray", "setURL", "setRowId", "setNString", "setNCharacterStream", "setNClob",
                "setSQLXML"
        );
        private final StatementFormatter statementFormatter;

        public PreparedStatementHandler(Connection connection, PreparedStatement statement, String query) {
            super(connection, statement);
            this.statementFormatter = createStatementFormatter(getTarget(), query);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (SET_METHODS.contains(method.getName())) {
                return StatementLoggingConnectionProvider.this.invokeSet(this, proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }

        public Object invokeSet(Method method, Object[] args) throws Throwable {
            if (args != null) {
                Object parameter = args[0];
                if (parameter instanceof Integer) {
                    Object value = SET_NULL.equals(method.getName()) ? null : args[1];
                    statementFormatter.setParameter(((Integer) parameter) - 1, value);
                }
            }
            return invokeTarget(method, args);
        }

        @Override
        public Object invokeExecute(Method method, Object[] args) throws Throwable {
            log(statementFormatter.format());
            return invokeTarget(method, args);
        }
    }

    @Override
    public String toString() {
        return connectionProvider.toString();
    }
}
