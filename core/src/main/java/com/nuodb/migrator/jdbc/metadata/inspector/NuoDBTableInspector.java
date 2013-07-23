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

import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TABLE;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.metadata.inspector.NuoDBInspectorUtils.validate;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class NuoDBTableInspector extends InspectorBase<Schema, TableInspectionScope> {

    private static final String QUERY = "SELECT * FROM SYSTEM.TABLES";

    public NuoDBTableInspector() {
        super(TABLE, SCHEMA, TableInspectionScope.class);
    }

    @Override
    public void inspectScope(final InspectionContext inspectionContext, final TableInspectionScope inspectionScope) throws SQLException {
        validate(inspectionScope);
        final Collection<String> filters = newArrayList();
        final Collection<String> parameters = newArrayList();

        String schemaName = inspectionScope.getSchema();
        if (schemaName != null) {
            filters.add(containsAny(schemaName, "%_") ? "SCHEMA LIKE ?" : "SCHEMA=?");
            parameters.add(schemaName);
        }

        String tableName = inspectionScope.getTable();
        if (tableName != null) {
            filters.add(containsAny(tableName, "%_") ? "TABLENAME LIKE ?" : "TABLENAME=?");
            parameters.add(tableName);
        }

        String[] tableTypes = inspectionScope.getTableTypes();
        if (tableTypes != null && tableTypes.length > 0) {
            StringBuilder filter = new StringBuilder("TYPE IN");
            filter.append(' ');
            filter.append('(');
            for (int i = 0, length = tableTypes.length; i < length; i++) {
                String tableType = tableTypes[i];
                filter.append('?');
                if ((i + 1) != length) {
                    filter.append(", ");
                }
                parameters.add(tableType);
            }
            filter.append(')');
            filters.add(filter.toString());
        }

        final String query = where(QUERY, filters, "AND");
        final StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.execute(
                new StatementFactory<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(query,
                                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void process(PreparedStatement statement) throws SQLException {
                        int parameter = 1;
                        for (Iterator<String> iterator = parameters.iterator(); iterator.hasNext(); ) {
                            statement.setString(parameter++, iterator.next());
                        }
                        inspect(inspectionContext, statement.executeQuery());
                    }
                }
        );
    }

    @Override
    public void inspectObjects(final InspectionContext inspectionContext,
                               final Collection<? extends Schema> schemas) throws SQLException {
        final String query = where(QUERY, newArrayList("SCHEMA=?"), "AND");
        final StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.execute(
                new StatementFactory<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(query,
                                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void process(PreparedStatement statement) throws SQLException {
                        for (Schema schema : schemas) {
                            statement.setString(1, schema.getName());
                            inspect(inspectionContext, statement.executeQuery());
                        }
                    }
                }
        );
    }

    private void inspect(InspectionContext inspectionContext, ResultSet tables) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        while (tables.next()) {
            Table table = addTable(inspectionResults, null, tables.getString("SCHEMA"), tables.getString("TABLENAME"));
            table.setType(tables.getString("TYPE"));
            table.setComment(tables.getString("REMARKS"));
        }
    }
}
