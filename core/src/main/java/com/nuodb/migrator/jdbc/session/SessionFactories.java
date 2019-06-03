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
package com.nuodb.migrator.jdbc.session;

import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.spec.ConnectionSpec;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class SessionFactories {
    public static SessionFactory newSessionFactory(final Dialect dialect, final ConnectionSpec connectionSpec) {
        return new SessionFactoryBase() {
            @Override
            protected Session open(Map<Object, Object> context) throws SQLException {
                return new SessionBase(this, null, dialect, context, false) {
                    @Override
                    public ConnectionSpec getConnectionSpec() {
                        return connectionSpec;
                    }

                    @Override
                    public Connection getConnection() {
                        throw new SessionException("Connection less session");
                    }
                };
            }

            @Override
            protected void close(Session session) throws SQLException {
            }
        };
    }

    public static SessionFactory newSessionFactory(final ConnectionProvider connectionProvider, final Dialect dialect,
            boolean enforceTableLocksForDDL) {
        return new SessionFactoryBase() {
            @Override
            protected SessionBase open(Map<Object, Object> context) throws SQLException {
                return new SessionBase(this, connectionProvider.getConnection(), dialect, context,
                        enforceTableLocksForDDL);
            }

            @Override
            protected void close(Session session) throws SQLException {
                connectionProvider.closeConnection(session.getConnection());
            }
        };
    }

    public static SessionFactory newSessionFactory(final ConnectionProvider connectionProvider,
            final DialectResolver dialectResolver) {
        return new SessionFactoryBase() {

            private Dialect dialect;

            @Override
            protected SessionBase open(Map<Object, Object> context) throws SQLException {
                Connection connection = connectionProvider.getConnection();
                if (dialect == null) {
                    try {
                        dialect = dialectResolver.resolve(connection);
                    } catch (SQLException exception) {
                        connection.close();
                        throw exception;
                    }
                }
                boolean enforceTableLocksForDDL = checkEnforcedTableLocks(connection);
                return new SessionBase(this, connection, dialect, context, enforceTableLocksForDDL);
            }

            @Override
            protected void close(Session session) throws SQLException {
                connectionProvider.closeConnection(session.getConnection());
            }
        };
    }

    protected static boolean checkEnforcedTableLocks(Connection connection) {
        Statement statement;
        try {
            statement = connection.createStatement();
            ResultSet rs = statement
                    .executeQuery("SELECT value FROM system.properties WHERE property='ENFORCE_TABLE_LOCKS_FOR_DDL';");
            if (rs.next()) {
                return rs.getBoolean("VALUE");
            }
        } catch (SQLException e) {
            try {
                connection.commit();
            } catch (SQLException e1) {
                return false;
            }
            return false;
        }
        return false;
    }
}
