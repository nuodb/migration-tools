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
package com.nuodb.tools.migration.jdbc;

import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.connection.DriverManagerConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.DatabaseIntrospector;
import com.nuodb.tools.migration.jdbc.type.extract.JdbcTypeExtractor;
import com.nuodb.tools.migration.jdbc.type.extract.JdbcTypeExtractorImpl;
import com.nuodb.tools.migration.jdbc.type.jdbc4.Jdbc4Types;
import com.nuodb.tools.migration.spec.ConnectionSpec;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;

/**
 * @author Sergey Bushik
 */
public class JdbcServicesImpl implements JdbcServices {

    private ConnectionSpec connectionSpec;
    private ConnectionProvider connectionProvider;
    private JdbcTypeExtractor jdbcTypeExtractor;

    public JdbcServicesImpl(DriverManagerConnectionSpec connectionSpec) {
        this(connectionSpec, new DriverManagerConnectionProvider(connectionSpec, false, TRANSACTION_READ_COMMITTED));
    }

    public JdbcServicesImpl(ConnectionSpec connectionSpec, ConnectionProvider connectionProvider) {
        this(connectionSpec, connectionProvider, new JdbcTypeExtractorImpl(Jdbc4Types.INSTANCE));
    }

    public JdbcServicesImpl(ConnectionSpec connectionSpec, ConnectionProvider connectionProvider, JdbcTypeExtractor jdbcTypeExtractor) {
        this.connectionSpec = connectionSpec;
        this.connectionProvider = connectionProvider;
        this.jdbcTypeExtractor = jdbcTypeExtractor;
    }

    @Override
    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    @Override
    public JdbcTypeExtractor getJdbcTypeExtractor() {
        return jdbcTypeExtractor;
    }

    @Override
    public DatabaseIntrospector getDatabaseIntrospector() {
        DatabaseIntrospector introspector = new DatabaseIntrospector();
        introspector.withConnectionProvider(getConnectionProvider());
        introspector.withCatalog(connectionSpec.getCatalog());
        introspector.withSchema(connectionSpec.getSchema());
        return introspector;
    }
}
