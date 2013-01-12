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

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class MSSQLServerAutoIncrementReader extends MetaDataReaderBase {

    private static final String QUERY =
            "SELECT COLUMN_NAME,\n" +
            "IDENT_SEED(QUOTENAME(TABLE_CATALOG) + '.' + QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)) AS START_WITH,\n" +
            "IDENT_CURRENT(QUOTENAME(TABLE_CATALOG) + '.' + QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)) AS LAST_VALUE,\n" +
            "IDENT_INCR(QUOTENAME(TABLE_CATALOG) + '.' + QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)) AS INCREMENT_BY\n" +
            "FROM INFORMATION_SCHEMA.COLUMNS\n" +
            "WHERE COLUMNPROPERTY(OBJECT_ID(QUOTENAME(TABLE_CATALOG) + '.' + QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)), COLUMN_NAME, 'ISIDENTITY') = 1\n" +
            "  AND TABLE_CATALOG = ?\n" +
            "  AND TABLE_SCHEMA = ?\n" +
            "  AND TABLE_NAME = ?";


    public MSSQLServerAutoIncrementReader() {
        super(MetaDataType.AUTO_INCREMENT);
    }

    @Override
    public void read(final DatabaseInspector inspector, final Database database,
                     DatabaseMetaData databaseMetaData) throws SQLException {
        StatementTemplate template = new StatementTemplate(databaseMetaData.getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(QUERY, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        for (Table table : database.listTables()) {
                            statement.setString(1, table.getCatalog().getName());
                            statement.setString(2, table.getSchema().getName());
                            statement.setString(3, table.getName());
                            ResultSet resultSet = statement.executeQuery();
                            try {
                                read(table, resultSet);
                            } finally {
                                close(resultSet);
                            }
                        }
                    }
                }
        );
    }

    protected void read(Table table, ResultSet resultSet) throws SQLException {
        if (resultSet.next()) {
            Column column = table.createColumn(resultSet.getString("COLUMN_NAME"));
            column.setAutoIncrement(true);

            Sequence sequence = new Sequence();
            sequence.setStartWith(resultSet.getLong("START_WITH"));
            sequence.setLastValue(resultSet.getLong("LAST_VALUE"));
            sequence.setIncrementBy(resultSet.getLong("INCREMENT_BY"));
            column.setSequence(sequence);
        }
    }
}
