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

import com.nuodb.migration.jdbc.JdbcUtils;
import com.nuodb.migration.jdbc.metadata.Column;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.Table;
import com.nuodb.migration.jdbc.query.StatementCallback;
import com.nuodb.migration.jdbc.query.StatementCreator;
import com.nuodb.migration.jdbc.query.StatementTemplate;
import com.nuodb.migration.jdbc.type.JdbcTypeDesc;
import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;

import java.sql.*;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class NuoDBColumnReader extends NuoDBMetaDataReaderBase {

    private static final String QUERY =
            "SELECT *\n" +
            "FROM SYSTEM.FIELDS\n" +
            "INNER JOIN SYSTEM.DATATYPES ON FIELDS.DATATYPE = DATATYPES.ID\n" +
            "WHERE SCHEMA=?\n" +
            "  AND TABLENAME=?\n";

    public NuoDBColumnReader() {
        super(MetaDataType.COLUMN);
    }

    @Override
    protected void doRead(DatabaseInspector inspector, final Database database,
                          DatabaseMetaData databaseMetaData) throws SQLException {
        final StatementTemplate template = new StatementTemplate(databaseMetaData.getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(QUERY,
                                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        for (Table table : database.listTables()) {
                            statement.setString(1, table.getSchema().getName());
                            statement.setString(2, table.getName());
                            ResultSet columns = statement.executeQuery();
                            try {
                                readTable(table, columns);
                            } finally {
                                JdbcUtils.close(columns);
                            }
                        }
                    }
                }
        );
    }

    protected void readTable(Table table, ResultSet columns) throws SQLException {
        JdbcTypeRegistry typeRegistry = table.getDatabase().getDialect().getJdbcTypeRegistry();
        while (columns.next()) {
            Column column = table.createColumn(columns.getString("FIELD"));

            JdbcTypeDesc typeDescAlias = typeRegistry.getJdbcTypeDescAlias(
                    columns.getInt("JDBCTYPE"), columns.getString("NAME"));
            column.setTypeCode(typeDescAlias.getTypeCode());
            column.setTypeName(typeDescAlias.getTypeName());

            int columnSize = columns.getInt("LENGTH");
            column.setSize(columnSize);
            column.setPrecision(columnSize);
            column.setDefaultValue(columns.getString("DEFAULTVALUE"));
            column.setScale(columns.getInt("SCALE"));
            column.setComment(columns.getString("REMARKS"));
            column.setPosition(columns.getInt("FIELDPOSITION"));
            column.setNullable(columns.getInt("FLAGS") == 0);
            column.setAutoIncrement(columns.getString("GENERATOR_SEQUENCE") != null);
        }
    }
}
