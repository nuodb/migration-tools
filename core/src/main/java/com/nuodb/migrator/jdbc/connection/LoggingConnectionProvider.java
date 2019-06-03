/**
 * Copyright (c) 2015, NuoDB, Inc.
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
import com.nuodb.migrator.utils.aop.AopProxy;
import com.nuodb.migrator.utils.aop.MethodMatcher;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.utils.aop.AopProxyUtils.createAopProxy;
import static com.nuodb.migrator.utils.aop.MethodAdvisors.newMethodAdvisor;
import static com.nuodb.migrator.utils.aop.MethodMatchers.newMethodMatcher;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class LoggingConnectionProvider extends ConnectionProxyProviderBase {

    private static final String GET_META_DATA_METHOD = "getMetaData";
    private static final String GET_CONNECTION_METHOD = "getConnection";
    private static final String CREATE_STATEMENT_METHOD = "createStatement";
    private static final String PREPARE_STATEMENT_METHOD = "prepareStatement";
    private static final Collection<String> EXECUTE_METHODS = newArrayList("execute", "executeQuery", "executeUpdate");
    private static final String SET_NULL_METHOD = "setNull";
    private static final Collection<String> SET_METHODS = newArrayList("setNull", "setBoolean", "setByte", "setShort",
            "setInt", "setLong", "setFloat", "setDouble", "setBigDecimal", "setString", "setBytes", "setDate",
            "setTime", "setTimestamp", "setAsciiStream", "setUnicodeStream", "setBinaryStream", "setObject",
            "setCharacterStream", "setRef", "setBlob", "setClob", "setArray", "setURL", "setRowId", "setNString",
            "setNCharacterStream", "setNClob", "setSQLXML");

    private final ConnectionProvider connectionProvider;
    private final QueryLogger queryLogger;
    private final QueryFormatFactory queryFormatFactory;

    public LoggingConnectionProvider(ConnectionProvider connectionProvider) {
        this(connectionProvider, new SimpleQueryFormatFactory());
    }

    public LoggingConnectionProvider(ConnectionProvider connectionProvider, QueryFormatFactory queryFormatFactory) {
        this(connectionProvider, queryFormatFactory, new SimpleQueryLogger(getLogger(LoggingConnectionProvider.class)));
    }

    public LoggingConnectionProvider(ConnectionProvider connectionProvider, QueryFormatFactory queryFormatFactory,
            QueryLogger queryLogger) {
        this.connectionProvider = connectionProvider;
        this.queryFormatFactory = queryFormatFactory;
        this.queryLogger = queryLogger;
    }

    @Override
    public ConnectionSpec getConnectionSpec() {
        return connectionProvider.getConnectionSpec();
    }

    @Override
    protected void initConnection(Connection connection) throws SQLException {
        if (connectionProvider instanceof ConnectionProviderBase) {
            ((ConnectionProviderBase) connectionProvider).initConnection(connection);
        }
    }

    @Override
    protected void initConnectionProxy(final AopProxy connection) {
        super.initConnectionProxy(connection);

        connection.addAdvisor(newMethodAdvisor(new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                AopProxy metaData = createAopProxy(invocation.proceed(), DatabaseMetaData.class);
                initMetaDataProxy(connection, metaData);
                return metaData;
            }
        }, newMethodMatcher(Connection.class, GET_META_DATA_METHOD)));

        // statement advices
        connection.addAdvisor(newMethodAdvisor(new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                final AopProxy statement = createAopProxy(invocation.proceed(), Statement.class);
                initStatementProxy(connection, statement);
                return statement;
            }
        }, new MethodMatcher() {
            @Override
            public boolean matches(Method method, Class<?> targetClass, Object[] arguments) {
                return method.getName().startsWith(CREATE_STATEMENT_METHOD);
            }
        }));

        // prepared statement advices
        connection.addAdvisor(newMethodAdvisor(new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                final AopProxy statement = createAopProxy(invocation.proceed(), PreparedStatement.class);
                initPreparedStatementProxy(connection, statement, (String) invocation.getArguments()[0]);
                return statement;
            }
        }, new MethodMatcher() {
            @Override
            public boolean matches(Method method, Class<?> targetClass, Object[] arguments) {
                return method.getName().startsWith(PREPARE_STATEMENT_METHOD);
            }
        }));
    }

    protected void initMetaDataProxy(final AopProxy connection, final AopProxy metaData) {
        // metaData.getConnection() returns proxy
        metaData.addAdvisor(newMethodAdvisor(new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                return connection;
            }
        }, newMethodMatcher(DatabaseMetaData.class, GET_CONNECTION_METHOD)));
    }

    protected void initStatementProxy(final AopProxy connection, final AopProxy statement) {
        // statement.getConnection() returns proxy connection
        statement.addAdvisor(newMethodAdvisor(new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                return connection;
            }
        }, newMethodMatcher(Statement.class, GET_CONNECTION_METHOD)));

        // statement.execute(query), statement.executeQuery(query),
        // statement.executeUpdate(query) logs query string
        statement.addAdvisor(newMethodAdvisor(new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                log((Statement) statement, (String) invocation.getArguments()[0]);
                return invocation.proceed();
            }
        }, new MethodMatcher() {
            @Override
            public boolean matches(Method method, Class<?> targetClass, Object[] arguments) {
                return method.getDeclaringClass() == Statement.class && EXECUTE_METHODS.contains(method.getName());
            }
        }));
    }

    protected void initPreparedStatementProxy(final AopProxy connection, final AopProxy statement, final String query) {
        initStatementProxy(connection, statement);

        final QueryFormat queryFormat = createQueryFormat((Statement) statement, query);
        // statement.setXXX() capture parameters
        statement.addAdvisor(newMethodAdvisor(new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                Object parameter = arguments[0];
                if (parameter instanceof Integer) {
                    Object value = SET_NULL_METHOD.equals(invocation.getMethod().getName()) ? null : arguments[1];
                    queryFormat.setParameter(((Integer) parameter) - 1, value);
                }
                return invocation.proceed();
            }
        }, new MethodMatcher() {
            @Override
            public boolean matches(Method method, Class<?> targetClass, Object[] arguments) {
                return method.getDeclaringClass() == PreparedStatement.class && SET_METHODS.contains(method.getName());
            }
        }));
        // statement.execute(), statement.executeQuery(),
        // statement.executeUpdate() logs query string
        statement.addAdvisor(newMethodAdvisor(new MethodInterceptor() {
            @Override
            public Object invoke(MethodInvocation invocation) throws Throwable {
                log((Statement) statement, queryFormat.format());
                return invocation.proceed();
            }
        }, new MethodMatcher() {
            @Override
            public boolean matches(Method method, Class<?> targetClass, Object[] arguments) {
                return method.getDeclaringClass() == PreparedStatement.class
                        && EXECUTE_METHODS.contains(method.getName());
            }
        }));
    }

    protected QueryFormat createQueryFormat(Statement statement, String query) {
        return queryFormatFactory.createQueryFormat(statement, query);
    }

    protected void log(Statement statement, String query) {
        log(createQueryFormat(statement, query).format());
    }

    protected void log(String statement) {
        queryLogger.log(statement);
    }

    @Override
    protected Connection createConnection() throws SQLException {
        if (connectionProvider instanceof ConnectionProxyProviderBase) {
            return ((ConnectionProxyProviderBase) connectionProvider).createConnection();
        } else {
            return connectionProvider.getConnection();
        }
    }

    @Override
    protected Connection getConnection(Connection connection) {
        if (connectionProvider instanceof ConnectionProxyProviderBase) {
            return ((ConnectionProxyProviderBase) connectionProvider).getConnection(connection);
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
}
