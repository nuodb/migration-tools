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
package com.nuodb.migration.jdbc.metadata.inspector;

import com.nuodb.migration.jdbc.metadata.*;
import com.nuodb.migration.jdbc.query.StatementCallback;
import com.nuodb.migration.jdbc.query.StatementCreator;
import com.nuodb.migration.jdbc.query.StatementTemplate;

import java.sql.*;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class NuoDBPrimaryKeyReader extends NuoDBMetaDataReaderBase implements NuoDBIndex {

    public NuoDBPrimaryKeyReader() {
        super(MetaDataType.PRIMARY_KEY);
    }

    @Override
    protected void doRead(DatabaseInspector inspector, final Database database,
                          DatabaseMetaData databaseMetaData) throws SQLException {
        final StatementTemplate template = new StatementTemplate(databaseMetaData.getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        StringBuilder query = new StringBuilder(QUERY);
                        query.append(' ');
                        query.append("AND");
                        query.append(' ');
                        query.append("INDEXTYPE=").append(PRIMARY_KEY);
                        return connection.prepareStatement(query.toString(),
                                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        for (Table table : database.listTables()) {
                            statement.setString(1, table.getSchema().getName());
                            statement.setString(2, table.getName());
                            readIndexes(table, statement.executeQuery());
                        }
                    }
                }
        );
    }

    protected void readIndexes(Table table, ResultSet primaryKeys) throws SQLException {
        while (primaryKeys.next()) {
            final Identifier identifier = Identifier.valueOf(primaryKeys.getString("INDEXNAME"));
            PrimaryKey primaryKey = table.getPrimaryKey();
            if (primaryKey == null) {
                table.setPrimaryKey(primaryKey = new PrimaryKey(identifier));
            }
            primaryKey.addColumn(table.createColumn(primaryKeys.getString("FIELD")), primaryKeys.getInt("POSITION"));
        }
    }
}
