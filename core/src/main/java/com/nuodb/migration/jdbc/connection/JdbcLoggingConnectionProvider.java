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

import com.nuodb.migration.jdbc.query.SimpleStatementFormatterFactory;
import com.nuodb.migration.jdbc.query.StatementFormatter;
import com.nuodb.migration.jdbc.query.StatementFormatterFactory;
import com.nuodb.migration.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.Collection;

import static com.google.common.collect.Sets.newHashSet;

/**
 * @author Sergey Bushik
 */
public class JdbcLoggingConnectionProvider extends ConnectionProviderBase {

    private StatementFormatterFactory statementFormatterFactory;
    private ConnectionProvider connectionProvider;
    private Logger logger;

    public JdbcLoggingConnectionProvider(ConnectionProvider connectionProvider) {
        this(connectionProvider, new SimpleStatementFormatterFactory());
    }

    public JdbcLoggingConnectionProvider(ConnectionProvider connectionProvider,
                                         StatementFormatterFactory statementFormatterFactory) {
        this(connectionProvider, LoggerFactory.getLogger(JdbcLoggingConnectionProvider.class),
                statementFormatterFactory);
    }

    public JdbcLoggingConnectionProvider(ConnectionProvider connectionProvider, Logger logger) {
        this(connectionProvider, logger, new SimpleStatementFormatterFactory());
    }

    public JdbcLoggingConnectionProvider(ConnectionProvider connectionProvider, Logger logger,
                                         StatementFormatterFactory statementFormatterFactory) {
        this.connectionProvider = connectionProvider;
        this.logger = logger;
        this.statementFormatterFactory = statementFormatterFactory;
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
        return connectionProvider.getConnection();
    }

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        connectionProvider.closeConnection(connection);
    }

    @Override
    protected ConnectionProxy createConnectionProxy(Connection connection) {
        return (ConnectionProxy) Proxy.newProxyInstance(ReflectionUtils.getClassLoader(),
                new Class<?>[]{ConnectionProxy.class}, new ConnectionInvocationHandler(connection));
    }

    protected void log(String query) {
        if (query.startsWith("INSERT INTO \"HOCKEY\".\"SiteTree_Live\" (\"ID\", \"ClassName\", \"Created\", \"LastEdited\", \"URLSegment\", \"Title\", \"MenuTitle\", \"Content\", \"MetaTitle\", \"MetaDescription\", \"MetaKeywords\", \"ExtraMeta\", \"ShowInMenus\", \"ShowInSearch\", \"Sort\", \"HasBrokenFile\", \"HasBrokenLink\", \"ReportClass\", \"CanViewType\", \"CanEditType\", \"Version\", \"ParentID\", \"Priority\") VALUES ('10'")) {
            System.out.println();
        }
        if (logger.isTraceEnabled()) {
            logger.trace(query);
        }
    }

    protected Object invokeConnectionGetMetaData(TargetInvocationHandler<Connection> targetInvocationHandler,
                                                 Object proxy, Method method, Object[] args) {
        DatabaseMetaData databaseMetaData = targetInvocationHandler.invokeTarget(method, args);
        return Proxy.newProxyInstance(ReflectionUtils.getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                new ConnectionAwareInvocationHandler((Connection) proxy, databaseMetaData));
    }

    protected Object invokeConnectionCreateStatement(TargetInvocationHandler<Connection> targetInvocationHandler,
                                                     Object proxy, Method method, Object[] args) {
        Statement statement = targetInvocationHandler.invokeTarget(method, args);
        return Proxy.newProxyInstance(ReflectionUtils.getClassLoader(), new Class<?>[]{Statement.class},
                new StatementInvocationHandler((Connection) proxy, statement));
    }

    protected Object invokeConnectionPrepareStatement(TargetInvocationHandler<Connection> targetInvocationHandler,
                                                      Object proxy, Method method, Object[] args) {
        PreparedStatement preparedStatement = targetInvocationHandler.invokeTarget(method, args);
        return Proxy.newProxyInstance(ReflectionUtils.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                new PreparedStatementInvocationHandler((Connection) proxy, preparedStatement,
                        (String) args[0]));
    }

    protected Object invokeStatementExecute(TargetInvocationHandler<? extends Statement> targetInvocationHandler,
                                            Object proxy, Method method, Object[] args) {
        return ((StatementInvocationHandler) targetInvocationHandler).invokeExecute(method, args);
    }

    protected Object invokePreparedStatementSet(TargetInvocationHandler<PreparedStatement> targetInvocationHandler,
                                                Object proxy, Method method, Object[] args) {
        return ((PreparedStatementInvocationHandler) targetInvocationHandler).invokeSet(method, args);
    }

    protected class ConnectionInvocationHandler extends ConnectionProviderBase.ConnectionInvocationHandler {

        private static final String GET_META_DATA_METHOD = "getMetaData";

        private static final String CREATE_STATEMENT_METHOD = "createStatement";

        private static final String PREPARE_STATEMENT_METHOD = "prepareStatement";

        public ConnectionInvocationHandler(Connection connection) {
            super(connection);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (CREATE_STATEMENT_METHOD.equals(method.getName())) {
                return invokeConnectionCreateStatement(this, proxy, method, args);
            } else if (PREPARE_STATEMENT_METHOD.equals(method.getName()) && args != null) {
                return invokeConnectionPrepareStatement(this, proxy, method, args);
            } else if (GET_META_DATA_METHOD.equals(method.getName())) {
                return invokeConnectionGetMetaData(this, proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }
    }

    protected class ConnectionAwareInvocationHandler<T> extends TargetInvocationHandler<T> {

        private static final String GET_CONNECTION_METHOD = "getConnection";

        private Connection connection;

        public ConnectionAwareInvocationHandler(Connection connection, T target) {
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

    public class StatementInvocationHandler<T extends Statement> extends ConnectionAwareInvocationHandler<T> {

        private final Collection<String> EXECUTE_METHODS = newHashSet("execute", "executeQuery", "executeUpdate");

        public StatementInvocationHandler(Connection connection, T statement) {
            super(connection, statement);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (EXECUTE_METHODS.contains(methodName)) {
                return invokeStatementExecute(this, proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }

        public Object invokeExecute(Method method, Object[] args) {
            log(statementFormatterFactory.createStatementFormatter(getTarget(), (String) args[0]).format());
            return invokeTarget(method, args);
        }
    }

    protected class PreparedStatementInvocationHandler extends StatementInvocationHandler<PreparedStatement> {

        private static final String SET_NULL = "setNull";

        private final Collection<String> SET_METHODS = newHashSet(
                "setNull", "setBoolean", "setByte", "setShort", "setInt", "setLong", "setFloat", "setDouble",
                "setBigDecimal", "setString", "setBytes", "setDate", "setTime", "setTimestamp", "setAsciiStream",
                "setUnicodeStream", "setBinaryStream", "setObject", "setCharacterStream", "setRef", "setBlob",
                "setClob", "setArray", "setURL", "setRowId", "setNString", "setNCharacterStream", "setNClob",
                "setSQLXML"
        );
        private final StatementFormatter statementFormatter;


        public PreparedStatementInvocationHandler(Connection connection,
                                                  PreparedStatement preparedStatement, String query) {
            super(connection, preparedStatement);
            this.statementFormatter = statementFormatterFactory.createStatementFormatter(getTarget(), query);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (SET_METHODS.contains(method.getName())) {
                return invokePreparedStatementSet(this, proxy, method, args);
            } else {
                return super.invoke(proxy, method, args);
            }
        }

        public Object invokeSet(Method method, Object[] args) {
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
        public Object invokeExecute(Method method, Object[] args) {
            log(statementFormatter.format());
            return invokeTarget(method, args);
        }
    }
}
