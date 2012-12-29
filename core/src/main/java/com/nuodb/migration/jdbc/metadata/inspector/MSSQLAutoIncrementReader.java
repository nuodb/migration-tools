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

import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.*;
import com.nuodb.migration.jdbc.query.StatementCallback;
import com.nuodb.migration.jdbc.query.StatementCreator;
import com.nuodb.migration.jdbc.query.StatementTemplate;

import java.sql.*;

import static com.nuodb.migration.jdbc.JdbcUtils.close;

/**
 * @author Sergey Bushik
 */
public class MSSQLAutoIncrementReader extends MetaDataReaderBase {

    private static final String QUERY = "SELECT NAME AS COLUMN_NAME,\n" +
            "IDENT_CURRENT(OBJECT_NAME(OBJECT_ID)) AS START_WITH,\n" +
            "IDENT_SEED(OBJECT_NAME(OBJECT_ID))    AS LAST_VALUE,\n" +
            "IDENT_INCR(OBJECT_NAME(OBJECT_ID))    AS INCREMENT_BY\n" +
            "FROM   SYS.IDENTITY_COLUMNS\n" +
            "WHERE  OBJECT_ID = OBJECT_ID(?) AND IS_IDENTITY = 1";

    public MSSQLAutoIncrementReader() {
        super(MetaDataType.AUTO_INCREMENT);
    }

    @Override
    public void read(DatabaseInspector inspector, Database database, DatabaseMetaData metaData) throws SQLException {
        Connection connection = metaData.getConnection();
        String catalog = connection.getCatalog();
        try {
            readIdentityColumns(inspector, database, metaData);
        } finally {
            connection.setCatalog(catalog);
        }
    }

    protected void readIdentityColumns(final DatabaseInspector inspector, final Database database,
                                       DatabaseMetaData metaData) throws SQLException {
        StatementTemplate template = new StatementTemplate(metaData.getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(QUERY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        Connection connection = statement.getConnection();
                        for (Catalog catalog : database.listCatalogs()) {
                            try {
                                connection.setCatalog(catalog.getName());
                            } catch (SQLException exception) {
                                if (logger.isWarnEnabled()) {
                                    logger.warn("Can't switch catalog", exception);
                                }
                                continue;
                            }
                            readIdentityColumns(inspector, statement, catalog);
                        }
                    }


                }
        );
    }

    protected void readIdentityColumns(DatabaseInspector inspector, PreparedStatement statement,
                                       Catalog catalog) throws SQLException {
        Dialect dialect = catalog.getDatabase().getDialect();
        for (Table table : catalog.listTables()) {
            statement.setString(1, table.getQualifiedName(dialect));
            ResultSet resultSet = statement.executeQuery();
            try {
                readIdentityColumns(table, resultSet);
            } finally {
                close(resultSet);
            }
        }
    }

    protected void readIdentityColumns(Table table, ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
            Column column = table.createColumn(resultSet.getString("COLUMN_NAME"));
            column.setAutoIncrement(true);
            Sequence sequence = new Sequence();
            // TODO: generate or read sequence name
            sequence.setStartWith(resultSet.getLong("START_WITH"));
            sequence.setLastValue(resultSet.getLong("LAST_VALUE"));
            sequence.setIncrementBy(resultSet.getLong("INCREMENT_BY"));
            column.setSequence(sequence);
        }
    }
}
