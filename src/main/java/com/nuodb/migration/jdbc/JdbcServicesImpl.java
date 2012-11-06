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
package com.nuodb.migration.jdbc;

import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.DriverManagerConnectionProvider;
import com.nuodb.migration.jdbc.metamodel.DatabaseInspector;
import com.nuodb.migration.jdbc.type.access.JdbcTypeAccessor;
import com.nuodb.migration.jdbc.type.access.JdbcTypeAccessorImpl;
import com.nuodb.migration.spec.ConnectionSpec;
import com.nuodb.migration.spec.DriverManagerConnectionSpec;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;

/**
 * @author Sergey Bushik
 */
public class JdbcServicesImpl implements JdbcServices {

    private ConnectionSpec connectionSpec;
    private ConnectionProvider connectionProvider;
    private JdbcTypeAccessor jdbcTypeAccessor;

    public JdbcServicesImpl(DriverManagerConnectionSpec connectionSpec) {
        this(connectionSpec, new DriverManagerConnectionProvider(connectionSpec, false, TRANSACTION_READ_COMMITTED));
    }

    public JdbcServicesImpl(ConnectionSpec connectionSpec, ConnectionProvider connectionProvider) {
        this(connectionSpec, connectionProvider, new JdbcTypeAccessorImpl());
    }

    public JdbcServicesImpl(ConnectionSpec connectionSpec, ConnectionProvider connectionProvider,
                            JdbcTypeAccessor jdbcTypeAccessor) {
        this.connectionSpec = connectionSpec;
        this.connectionProvider = connectionProvider;
        this.jdbcTypeAccessor = jdbcTypeAccessor;
    }

    @Override
    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    @Override
    public JdbcTypeAccessor getJdbcTypeAccessor() {
        return jdbcTypeAccessor;
    }

    @Override
    public DatabaseInspector getDatabaseIntrospector() {
        DatabaseInspector inspector = new DatabaseInspector();
        inspector.withConnectionProvider(getConnectionProvider());
        inspector.withCatalog(connectionSpec.getCatalog());
        inspector.withSchema(connectionSpec.getSchema());
        return inspector;
    }
}
