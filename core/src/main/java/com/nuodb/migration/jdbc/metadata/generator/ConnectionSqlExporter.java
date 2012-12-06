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
package com.nuodb.migration.jdbc.metadata.generator;

import com.nuodb.migration.jdbc.JdbcUtils;
import com.nuodb.migration.jdbc.connection.ConnectionServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class ConnectionSqlExporter implements SqlExporter {

    private final transient Logger logger = LoggerFactory.getLogger(getClass());
    private ConnectionServices connectionServices;
    private Connection connection;
    private Statement statement;

    public ConnectionSqlExporter(ConnectionServices connectionServices) {
        this.connectionServices = connectionServices;
    }

    @Override
    public void open() throws Exception {
        connection = connectionServices.getConnection();
        statement = connection.createStatement();
    }

    @Override
    public void export(String[] queries) throws Exception {
        if (connection == null) {
            throw new ScriptGeneratorException("Database connection is not opened");
        }
        if (queries == null) {
            return;
        }
        for (String script : queries) {
            statement.executeUpdate(script);
            processWarning(statement.getWarnings());
        }
    }

    protected void processWarning(SQLWarning warning) throws SQLException {
        while (warning != null) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Warning code: %d, state: %s", warning.getErrorCode(), warning.getSQLState()));
            }
            warning = warning.getNextWarning();
        }
        connection.clearWarnings();
    }

    @Override
    public void close() throws Exception {
        JdbcUtils.close(connectionServices);
        JdbcUtils.close(statement);
    }
}
