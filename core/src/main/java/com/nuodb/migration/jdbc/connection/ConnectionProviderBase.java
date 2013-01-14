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

import com.nuodb.migration.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import static com.nuodb.migration.utils.ReflectionUtils.invokeMethod;

/**
 * @author Sergey Bushik
 */
public abstract class ConnectionProviderBase implements ConnectionProvider {

    protected transient final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public ConnectionServices getConnectionServices() {
        return new ConnectionServicesBase(this);
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = createConnection();
        initConnection(connection);
        return createConnectionProxy(connection);
    }

    protected ConnectionProxy createConnectionProxy(Connection connection) {
        return (ConnectionProxy) Proxy.newProxyInstance(ReflectionUtils.getClassLoader(),
                new Class<?>[]{ConnectionProxy.class}, new ConnectionInvocationHandler(connection));
    }

    protected abstract Connection createConnection() throws SQLException;

    protected void initConnection(Connection connection) throws SQLException {
    }

    protected Connection invokeGetConnection(Connection connection) {
        return connection;
    }

    public abstract class TargetInvocationHandler<T> implements InvocationHandler {

        private final T target;

        public TargetInvocationHandler(T target) {
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

    public class ConnectionInvocationHandler extends TargetInvocationHandler<Connection> {

        private static final String GET_CONNECTION_METHOD = "getConnection";

        public ConnectionInvocationHandler(Connection connection) {
            super(connection);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (GET_CONNECTION_METHOD.equals(method.getName())) {
                return invokeGetConnection(getTarget());
            } else {
                return super.invoke(proxy, method, args);
            }
        }
    }
}
