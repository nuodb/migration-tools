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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migration.jdbc.metadata.Identifier.EMPTY_IDENTIFIER;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class NuoDBTableReader extends NuoDBMetaDataReaderBase {

    private static final String QUERY = "SELECT * FROM SYSTEM.TABLES";

    public NuoDBTableReader() {
        super(MetaDataType.TABLE);
    }

    @Override
    protected void doRead(DatabaseInspector inspector, final Database database,
                          DatabaseMetaData databaseMetaData) throws SQLException {
        final Collection<String> filters = newArrayList();
        final Collection<String> parameters = newArrayList();

        getQueryFiltersParams(inspector, filters, parameters);

        final StringBuilder query = new StringBuilder(QUERY);
        if (!filters.isEmpty()) {
            query.append(' ');
            query.append("WHERE");
            for (Iterator<String> iterator = filters.iterator(); iterator.hasNext(); ) {
                String filter = iterator.next();
                query.append(' ');
                query.append(filter);
                if (iterator.hasNext()) {
                    query.append(' ');
                    query.append("AND");
                }
            }
        }

        final StatementTemplate template = new StatementTemplate(databaseMetaData.getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return connection.prepareStatement(query.toString(),
                                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        int parameter = 1;
                        for (Iterator<String> iterator = parameters.iterator(); iterator.hasNext(); ) {
                            statement.setString(parameter, iterator.next());
                        }
                        readTables(database, statement.executeQuery());
                    }
                }
        );
    }

    protected void getQueryFiltersParams(DatabaseInspector inspector,
                                         Collection<String> filters, Collection<String> parameters) {
        String schema = inspector.getSchema();
        if (schema != null) {
            filters.add(containsAny(schema, "%_") ? "SCHEMA LIKE ?" : "SCHEMA=?");
            parameters.add(schema);
        }

        String table = inspector.getTable();
        if (table != null) {
            filters.add(containsAny(table, "%_") ? "TABLENAME LIKE ?" : "TABLENAME=?");
            parameters.add(table);
        }

        String[] tableTypes = inspector.getTableTypes();
        if (tableTypes != null && tableTypes.length > 0) {
            StringBuilder filter = new StringBuilder("IN");
            filter.append(' ');
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
    }

    protected void readTables(Database database, ResultSet tables) throws SQLException {
        Catalog catalog = database.createCatalog(EMPTY_IDENTIFIER);
        while (tables.next()) {
            Schema schema = catalog.createSchema(tables.getString("SCHEMA"));
            Table table = schema.createTable(tables.getString("TABLENAME"));
            table.setType(tables.getString("TYPE"));
            table.setComment(tables.getString("REMARKS"));
        }
    }
}
