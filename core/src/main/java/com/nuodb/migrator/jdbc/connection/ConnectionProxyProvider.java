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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import static com.nuodb.migrator.utils.ReflectionUtils.getClassLoader;
import static com.nuodb.migrator.utils.ReflectionUtils.invokeMethod;
import static java.lang.reflect.Proxy.*;

/**
 * @author Sergey Bushik
 */
public abstract class ConnectionProxyProvider implements ConnectionProvider {

    protected transient final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public ConnectionServices getConnectionServices() {
        return new SimpleConnectionServices(this);
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = createConnection();
        initConnection(connection);
        return wrapConnection(connection);
    }

    protected abstract Connection createConnection() throws SQLException;

    protected void initConnection(Connection connection) throws SQLException {
    }

    protected Connection wrapConnection(Connection connection) {
        return (Connection) newProxyInstance(
                getClassLoader(), new Class<?>[]{ConnectionProxy.class},
                new ConnectionHandler(connection));
    }

    public Connection unwrapConnection(Connection connection) {
        if (!isProxyClass(connection.getClass())) {
            return connection;
        }
        InvocationHandler invocationHandler = getInvocationHandler(connection);
        if (invocationHandler instanceof TargetHandler) {
            return (Connection) ((TargetHandler) invocationHandler).getTarget();
        }
        return connection;
    }

    public abstract class TargetHandler<T> implements InvocationHandler {

        private final T target;

        public TargetHandler(T target) {
            this.target = target;
        }

        public T getTarget() {
            return target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return invokeTarget(method, args);
        }

        public <R> R invokeTarget(Method method, Object[] args) {
            return invokeMethod(getTarget(), method, args);
        }
    }

    public class ConnectionHandler extends TargetHandler<Connection> {

        private static final String GET_CONNECTION_METHOD = "getConnection";

        public ConnectionHandler(Connection connection) {
            super(connection);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (GET_CONNECTION_METHOD.equals(method.getName())) {
                return unwrapConnection(getTarget());
            } else {
                return super.invoke(proxy, method, args);
            }
        }
    }
}
