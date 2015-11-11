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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public abstract class ConnectionProviderBase<C extends ConnectionSpec> implements ConnectionProvider<C> {

    protected transient final Logger logger = LoggerFactory.getLogger(getClass());

    private C connectionSpec;

    protected ConnectionProviderBase() {
    }

    protected ConnectionProviderBase(C connectionSpec) {
        this.connectionSpec = connectionSpec;
    }

    @Override
    public C getConnectionSpec() {
        return connectionSpec;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Opening connection %s", getConnectionSpec()));
        }
        Connection connection = openConnection();
        initConnection(connection);
        return connection;
    }

    protected void initConnection(Connection connection) throws SQLException {
        Integer transactionIsolation = getConnectionSpec().getTransactionIsolation();
        if (transactionIsolation != null) {
            connection.setTransactionIsolation(transactionIsolation);
        }
        Boolean autoCommit = getConnectionSpec().getAutoCommit();
        if (autoCommit != null) {
            connection.setAutoCommit(autoCommit);
        }
    }

    protected Connection openConnection() throws SQLException {
        return createConnection();
    }

    protected abstract Connection createConnection() throws SQLException;

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        if (connection != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Closing connection");
            }
            connection.close();
        }
    }

    @Override
    public abstract void close() throws SQLException;

    @Override
    public String toString() {
        return getConnectionSpec().toString();
    }
}
