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
package com.nuodb.migration.jdbc.dialect;

import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migration.jdbc.type.jdbc4.Jdbc4TypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

/**
 * @author Sergey Bushik
 */
public class StandardDialect implements DatabaseDialect {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final DatabaseMetaData metaData;

    public StandardDialect(DatabaseMetaData metaData) {
        this.metaData = metaData;
    }

    protected char openQuote() {
        return '"';
    }

    protected char closeQuote() {
        return '"';
    }

    @Override
    public String quote(String name) {
        if (name == null) {
            return null;
        }
        return openQuote() + name + closeQuote();
    }

    @Override
    public String getNoColumnsInsertClause() {
        return "values ()";
    }

    @Override
    public boolean supportsReadCatalogs() {
        return true;
    }

    @Override
    public boolean supportsReadSchemas() {
        return true;
    }

    @Override
    public boolean supportsSessionTimeZone() {
        return false;
    }

    @Override
    public void setSessionTimeZone(Connection connection, TimeZone timeZone) throws SQLException {
    }

    @Override
    public boolean supportsTransactionIsolation(int transactionIsolationLevel) throws SQLException {
        return metaData.supportsTransactionIsolationLevel(transactionIsolationLevel);
    }

    @Override
    public void setTransactionIsolation(Connection connection,
                                        int[] transactionIsolationLevels) throws SQLException {
        if (transactionIsolationLevels != null) {
            for (int transactionIsolationLevel : transactionIsolationLevels) {
                if (supportsTransactionIsolation(transactionIsolationLevel)) {
                    connection.setTransactionIsolation(transactionIsolationLevel);
                    return;
                }
            }
        }
    }

    @Override
    public JdbcTypeRegistry getJdbcTypeRegistry() {
        return Jdbc4TypeRegistry.INSTANCE;
    }

    @Override
    public void enableStreaming(Statement statement) throws SQLException {
    }
}
