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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SEQUENCE;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class MySQLSequenceInspector extends TableInspectorBase<Table, TableInspectionScope> {

    private static final String QUERY_TABLE = "SELECT TABLE_SCHEMA, TABLE_NAME, AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES";
    private static final String QUERY_COLUMN = "SHOW COLUMNS FROM `%s`.`%s` WHERE EXTRA='AUTO_INCREMENT'";

    public MySQLSequenceInspector() {
        super(SEQUENCE, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        StringBuilder query = new StringBuilder(QUERY_TABLE);
        Collection<String> filters = newArrayList();
        Collection<Object> parameters = newArrayList();
        String catalog = tableInspectionScope.getCatalog();
        if (catalog != null) {
            filters.add(containsAny(tableInspectionScope.getCatalog(), "%") ? "TABLE_SCHEMA LIKE ?" : "TABLE_SCHEMA=?");
            parameters.add(catalog);
        } else {
            filters.add("TABLE_SCHEMA=DATABASE()");
        }
        String table = tableInspectionScope.getTable();
        if (table != null) {
            filters.add(containsAny(tableInspectionScope.getCatalog(), "%") ? "TABLE_NAME LIKE ?" : "TABLE_NAME=?");
            parameters.add(table);
        }
        filters.add("AUTO_INCREMENT IS NOT NULL");
        where(query, filters, "AND");
        return new ParameterizedQuery(newQuery(query.toString()), parameters);
    }

    @Override
    protected void processResultSet(InspectionContext inspectionContext, ResultSet tables) throws SQLException {
        final InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        final Map<Table, Sequence> sequences = newHashMap();
        while (tables.next()) {
            Table table = addTable(inspectionResults, tables.getString("TABLE_SCHEMA"), null,
                    tables.getString("TABLE_NAME"));
            Sequence sequence = new Sequence();
            sequence.setLastValue(tables.getLong("AUTO_INCREMENT"));
            sequences.put(table, sequence);
        }
        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.executeStatement(new StatementFactory<Statement>() {
            @Override
            public Statement createStatement(Connection connection) throws SQLException {
                return connection.createStatement();
            }
        }, new StatementCallback<Statement>() {

            @Override
            public void executeStatement(Statement statement) throws SQLException {
                for (Map.Entry<Table, Sequence> sequenceEntry : sequences.entrySet()) {
                    Table table = sequenceEntry.getKey();
                    Sequence sequence = sequenceEntry.getValue();
                    ResultSet columns = statement
                            .executeQuery(format(QUERY_COLUMN, table.getCatalog().getName(), table.getName()));
                    Column column;
                    try {
                        column = columns.next() ? table.addColumn(columns.getString("FIELD")) : null;
                    } finally {
                        closeQuietly(columns);
                    }
                    if (column == null) {
                        continue;
                    }
                    column.setSequence(sequence);
                    column.getTable().getSchema().addSequence(sequence);
                    inspectionResults.addObject(sequence);
                }
            }
        });
    }
}
