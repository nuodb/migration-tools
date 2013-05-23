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

import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.utils.ReflectionInvocationHandler;

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
public class QueryLoggingConnectionProvider extends ConnectionProxyProviderBase {

    private final ConnectionProvider connectionProvider;
    private final QueryLogger queryLogger;
    private final QueryFormatterFactory queryFormatterFactory;

    public QueryLoggingConnectionProvider(ConnectionProvider connectionProvider) {
        this(connectionProvider, new SimpleQueryFormatterFactory());
    }

    public QueryLoggingConnectionProvider(ConnectionProvider connectionProvider,
                                          QueryFormatterFactory queryFormatterFactory) {
        this(connectionProvider, queryFormatterFactory,
                new SimpleQueryLogger(getLogger(QueryLoggingConnectionProvider.class)));
    }

    public QueryLoggingConnectionProvider(ConnectionProvider connectionProvider,
                                          QueryFormatterFactory queryFormatterFactory, QueryLogger queryLogger) {
        this.connectionProvider = connectionProvider;
        this.queryFormatterFactory = queryFormatterFactory;
        this.queryLogger = queryLogger;
    }

    @Override
    public ConnectionServices getConnectionServices() throws SQLException {
        return connectionProvider.getConnectionServices();
    }

    @Override
    public ConnectionSpec getConnectionSpec() {
        return connectionProvider.getConnectionSpec();
    }

    @Override
    protected void initConnection(Connection connection) throws SQLException {
        if (connectionProvider instanceof ConnectionProxyProviderBase) {
            ((ConnectionProxyProviderBase) connectionProvider).initConnection(connection);
        }
    }

    @Override
    protected ConnectionProxy createConnectionProxy(Connection connection) {
        return (ConnectionProxy) newProxyInstance(getClassLoader(),
                new Class<?>[]{ConnectionProxy.class}, new ConnectionInvocationHandler(connection));
    }

    @Override
    protected Connection createTargetConnection() throws SQLException {
        if (connectionProvider instanceof ConnectionProxyProviderBase) {
            return ((ConnectionProxyProviderBase) connectionProvider).createTargetConnection();
        } else {
            return connectionProvider.getConnection();
        }
    }

    @Override
    protected Connection getTargetConnection(Connection connection) {
        if (connectionProvider instanceof ConnectionProxyProviderBase) {
            return ((ConnectionProxyProviderBase) connectionProvider).getTargetConnection(connection);
        } else {
            return connection;
        }
    }

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        connectionProvider.closeConnection(connection);
    }

    @Override
    public void close() throws SQLException {
        connectionProvider.close();
    }

    @Override
    public String toString() {
        return connectionProvider.toString();
    }

    protected class ConnectionInvocationHandler extends
            ConnectionProxyProviderBase.ConnectionInvocationHandler {

        private static final String GET_META_DATA_METHOD = "getMetaData";

        private static final String CREATE_STATEMENT_METHOD = "createStatement";

        private static final String PREPARE_STATEMENT_METHOD = "prepareStatement";

        public ConnectionInvocationHandler(Connection connection) {
            super(connection);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (CREATE_STATEMENT_METHOD.equals(method.getName())) {
                return invokeCreateStatement(proxy, method, args);
            } else if (PREPARE_STATEMENT_METHOD.equals(method.getName()) && args != null) {
                return invokePrepareStatement(proxy, method, args);
            } else if (GET_META_DATA_METHOD.equals(method.getName())) {
                return invokeGetMetaData(proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }

        protected Object invokeCreateStatement(Object proxy, Method method, Object[] args) throws Throwable {
            Statement statement = (Statement) invokeTarget(method, args);
            return newProxyInstance(getClassLoader(), new Class<?>[]{Statement.class},
                    new ConnectionAwareStatementHandler((Connection) proxy, statement));
        }

        protected Object invokePrepareStatement(Object proxy, Method method, Object[] args) throws Throwable {
            PreparedStatement preparedStatement = (PreparedStatement) invokeTarget(method, args);
            return newProxyInstance(getClassLoader(), new Class<?>[]{PreparedStatement.class},
                    new ConnectionAwarePreparedStatementHandler(
                            (Connection) proxy, preparedStatement, (String) args[0]));
        }

        protected Object invokeGetMetaData(Object proxy, Method method, Object[] args) throws Throwable {
            DatabaseMetaData databaseMetaData = (DatabaseMetaData) invokeTarget(method, args);
            return newProxyInstance(getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                    new ConnectionAwareInvocationHandlerBase((Connection) proxy, databaseMetaData));
        }
    }

    protected class ConnectionAwareInvocationHandlerBase<T> extends ReflectionInvocationHandler<T> {

        private static final String GET_CONNECTION_METHOD = "getConnection";

        private Connection connection;

        public ConnectionAwareInvocationHandlerBase(Connection connection, T target) {
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

    protected class ConnectionAwareStatementHandler<T extends Statement> extends ConnectionAwareInvocationHandlerBase<T> {

        private final Collection<String> EXECUTE_METHODS = newHashSet("execute", "executeQuery", "executeUpdate");

        public ConnectionAwareStatementHandler(Connection connection, T statement) {
            super(connection, statement);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (EXECUTE_METHODS.contains(methodName)) {
                return invokeExecute(proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }

        protected Object invokeExecute(Object proxy, Method method, Object[] args) throws Throwable {
            log(getTarget(), (String) args[0]);
            return invokeTarget(method, args);
        }
    }

    protected class ConnectionAwarePreparedStatementHandler extends ConnectionAwareStatementHandler<PreparedStatement> {

        private static final String SET_NULL = "setNull";

        private final Collection<String> SET_METHODS = newHashSet(
                "setNull", "setBoolean", "setByte", "setShort", "setInt", "setLong", "setFloat", "setDouble",
                "setBigDecimal", "setString", "setBytes", "setDate", "setTime", "setTimestamp", "setAsciiStream",
                "setUnicodeStream", "setBinaryStream", "setObject", "setCharacterStream", "setRef", "setBlob",
                "setClob", "setArray", "setURL", "setRowId", "setNString", "setNCharacterStream", "setNClob",
                "setSQLXML"
        );
        private final QueryFormatter statementFormatter;

        public ConnectionAwarePreparedStatementHandler(Connection connection, PreparedStatement statement,
                                                       String query) {
            super(connection, statement);
            this.statementFormatter = createQueryFormatter(getTarget(), query);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (SET_METHODS.contains(method.getName())) {
                return invokeSet(proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }

        protected Object invokeSet(Object proxy, Method method, Object[] args) throws Throwable {
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
        protected Object invokeExecute(Object object, Method method, Object[] args) throws Throwable {
            log(statementFormatter.format());
            return invokeTarget(method, args);
        }
    }

    protected QueryFormatter createQueryFormatter(Statement statement, String query) {
        return queryFormatterFactory.createQueryFormatter(statement, query);
    }

    protected void log(Statement statement, String query) {
        log(createQueryFormatter(statement, query).format());
    }

    protected void log(String statement) {
        queryLogger.log(statement);
    }
}
