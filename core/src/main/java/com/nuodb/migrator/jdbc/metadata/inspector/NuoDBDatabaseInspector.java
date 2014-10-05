/**
 * Copyright (c) 2014, NuoDB, Inc.
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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.NuoDBDatabaseInfo;
import com.nuodb.migrator.jdbc.query.StatementAction;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Sergey Bushik
 */
public class NuoDBDatabaseInspector extends SimpleDatabaseInspector {

    private static final String QUERY = "SELECT GETEFFECTIVEPLATFORMVERSION() FROM DUAL";

    @Override
    protected DatabaseInfo getDatabaseInfo(InspectionContext inspectionContext)
            throws SQLException {
        Connection connection = inspectionContext.getConnection();
        DatabaseMetaData metaData = connection.getMetaData();
        NuoDBDatabaseInfo databaseInfo = new NuoDBDatabaseInfo();
        databaseInfo.setProductName(metaData.getDatabaseProductName());
        databaseInfo.setProductVersion(metaData.getDatabaseProductVersion());
        databaseInfo.setMajorVersion(metaData.getDatabaseMajorVersion());
        databaseInfo.setMinorVersion(metaData.getDatabaseMinorVersion());
        Integer protocolVersion = new StatementTemplate(inspectionContext.getConnection()).executeStatement(
                new StatementFactory<Statement>() {
                    @Override
                    public Statement createStatement(Connection connection) throws SQLException {
                        return connection.createStatement();
                    }
                },
                new StatementAction<Statement, Integer>() {
                    @Override
                    public Integer executeStatement(Statement statement) throws SQLException {
                        ResultSet resultSet = statement.executeQuery(QUERY);
                        return resultSet.next() ? resultSet.getInt(1) : null;
                    }
                }
        );
        databaseInfo.setProtocolVersion(protocolVersion);
        return databaseInfo;
    }
}
