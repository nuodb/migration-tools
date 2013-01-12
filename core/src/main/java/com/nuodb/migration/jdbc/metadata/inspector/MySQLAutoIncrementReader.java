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
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class MySQLAutoIncrementReader extends MetaDataReaderBase {

    private static final String QUERY =
            "SELECT AUTO_INCREMENT AS LAST_VALUE FROM INFORMATION_SCHEMA.TABLES\n" +
            "WHERE TABLE_SCHEMA=? && TABLE_NAME=?";

    public MySQLAutoIncrementReader() {
        super(MetaDataType.AUTO_INCREMENT);
    }

    @Override
    public void read(DatabaseInspector inspector, final Database database,
                     DatabaseMetaData databaseMetaData) throws SQLException {
        final Collection<Column> columns = newArrayList();
        for (Table table : database.listTables()) {
            for (Column column : table.getColumns()) {
                if (column.isAutoIncrement()) {
                    columns.add(column);
                }
            }
        }
        if (columns.isEmpty()) {
            return;
        }
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
                        for (Column column : columns) {
                            Table table = column.getTable();
                            statement.setString(1, table.getCatalog().getName());
                            statement.setString(2, table.getName());
                            ResultSet resultSet = statement.executeQuery();
                            try {
                                read(column, resultSet);
                            } finally {
                                close(resultSet);
                            }
                        }
                    }
                }
        );
    }

    protected void read(Column column, ResultSet resultSet) throws SQLException {
        if (resultSet.next()) {
            Sequence sequence = new Sequence();
            sequence.setLastValue(resultSet.getLong("LAST_VALUE"));
            column.setSequence(sequence);
        }
    }
}
