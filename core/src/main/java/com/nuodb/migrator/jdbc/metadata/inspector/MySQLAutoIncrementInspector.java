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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nuodb.migrator.jdbc.metadata.AutoIncrement;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.QueryUtils;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementCreator;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class MySQLAutoIncrementInspector extends TableInspectorBase<Table, TableInspectionScope> {

    public static final String QUERY_TABLE = "SELECT TABLE_SCHEMA, TABLE_NAME, AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES";
    public static final String QUERY_COLUMN = "SHOW COLUMNS FROM `%1$s`.`%2$s` WHERE EXTRA='AUTO_INCREMENT'";

    public MySQLAutoIncrementInspector() {
        super(MetaDataType.AUTO_INCREMENT, TableInspectionScope.class);
    }

    @Override
    protected Collection<? extends TableInspectionScope> createInspectionScopes(Collection<? extends Table> tables) {
        return createTableInspectionScopes(tables);
    }

    @Override
    protected void inspectScopes(final InspectionContext inspectionContext,
                                 final Collection<? extends TableInspectionScope> inspectionScopes) throws SQLException {
        for (TableInspectionScope inspectionScope : inspectionScopes) {
            final StringBuilder query = new StringBuilder(QUERY_TABLE);
            Collection<String> filters = Lists.newArrayList();
            final Collection<String> parameters = Lists.newArrayList();
            String catalogName = inspectionScope.getCatalog();
            if (catalogName != null) {
                filters.add(containsAny(inspectionScope.getCatalog(), "%") ? "TABLE_SCHEMA LIKE ?" : "TABLE_SCHEMA=?");
                parameters.add(catalogName);
            } else {
                filters.add("TABLE_SCHEMA=DATABASE()");
            }
            String tableName = inspectionScope.getTable();
            if (tableName != null) {
                filters.add(containsAny(inspectionScope.getCatalog(), "%") ? "TABLE_NAME LIKE ?" : "TABLE_NAME=?");
                parameters.add(tableName);
            }
            filters.add("AUTO_INCREMENT IS NOT NULL");
            QueryUtils.where(query, filters, "AND");
            StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
            template.execute(
                    new StatementCreator<PreparedStatement>() {
                        @Override
                        public PreparedStatement create(Connection connection) throws SQLException {
                            return connection.prepareStatement(query.toString());
                        }
                    },
                    new StatementCallback<PreparedStatement>() {
                        @Override
                        public void execute(PreparedStatement statement) throws SQLException {
                            int parameter = 1;
                            for (Iterator<String> iterator = parameters.iterator(); iterator.hasNext(); ) {
                                statement.setString(parameter++, iterator.next());
                            }
                            inspect(inspectionContext, statement.executeQuery());
                        }
                    }
            );
        }
    }

    private void inspect(final InspectionContext inspectionContext, ResultSet tables) throws SQLException {
        final InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        final Map<Table, AutoIncrement> autoIncrementMap = Maps.newHashMap();
        while (tables.next()) {
            Table table = addTable(inspectionResults, tables.getString("TABLE_SCHEMA"), null, tables.getString("TABLE_NAME"));
            AutoIncrement autoIncrement = new AutoIncrement();
            autoIncrement.setLastValue(tables.getLong("AUTO_INCREMENT"));
            autoIncrementMap.put(table, autoIncrement);
        }
        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.execute(
                new StatementCreator<Statement>() {
                    @Override
                    public Statement create(Connection connection) throws SQLException {
                        return connection.createStatement();
                    }
                },
                new StatementCallback<Statement>() {

                    @Override
                    public void execute(Statement statement) throws SQLException {
                        for (Map.Entry<Table, AutoIncrement> autoIncrementEntry : autoIncrementMap.entrySet()) {
                            Table table = autoIncrementEntry.getKey();
                            AutoIncrement autoIncrement = autoIncrementEntry.getValue();
                            ResultSet columns = statement.executeQuery(
                                    format(QUERY_COLUMN, table.getCatalog().getName(), table.getName()));
                            Column column;
                            try {
                                column = columns.next() ? table.addColumn(columns.getString("FIELD")) : null;
                            } finally {
                                close(columns);
                            }
                            if (column == null) {
                                continue;
                            }
                            column.setSequence(autoIncrement);
                            column.setAutoIncrement(true);
                            inspectionResults.addObject(autoIncrement);
                        }
                    }
                }
        );
    }
}
