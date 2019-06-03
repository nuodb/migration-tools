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

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SEQUENCE;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addSchema;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.query.QueryUtils.AND;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLSequenceInspector extends TableInspectorBase<Table, TableInspectionScope> {

    private static final String QUERY = "SELECT S.SEQUENCE_SCHEMA AS SCHEMA_NAME, S.SEQUENCE_NAME AS SEQUENCE_NAME,\n"
            + "C.TABLE_NAME AS TABLE_NAME, C.COLUMN_NAME AS COLUMN_NAME FROM INFORMATION_SCHEMA.SEQUENCES S\n"
            + "LEFT OUTER JOIN INFORMATION_SCHEMA.COLUMNS C ON\n"
            + "S.SEQUENCE_CATALOG=C.TABLE_CATALOG AND S.SEQUENCE_SCHEMA=C.TABLE_SCHEMA AND\n"
            + "S.SEQUENCE_NAME=SUBSTRING(C.COLUMN_DEFAULT FROM 'nextval[(]''(.*)''::.*[)]')";

    public PostgreSQLSequenceInspector() {
        super(SEQUENCE, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        Collection<String> filters = newArrayList();
        Collection<Object> parameters = newArrayList();
        if (tableInspectionScope.getSchema() != null) {
            filters.add("S.SEQUENCE_SCHEMA=?");
            parameters.add(tableInspectionScope.getSchema());
        }
        if (tableInspectionScope.getTable() != null) {
            filters.add("C.TABLE_NAME=?");
            parameters.add(tableInspectionScope.getTable());
        }
        return new ParameterizedQuery(newQuery(where(QUERY, filters, AND)), parameters);
    }

    @Override
    protected void processResultSet(final InspectionContext inspectionContext, final ResultSet sequences)
            throws SQLException {
        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.executeStatement(new StatementFactory<Statement>() {
            @Override
            public Statement createStatement(Connection connection) throws SQLException {
                return connection.createStatement();
            }
        }, new StatementCallback<Statement>() {
            @Override
            public void executeStatement(Statement statement) throws SQLException {
                while (sequences.next()) {
                    String schemaName = sequences.getString("SCHEMA_NAME");
                    String sequenceName = sequences.getString("SEQUENCE_NAME");
                    String tableName = sequences.getString("TABLE_NAME");
                    Schema schema = addSchema(inspectionContext.getInspectionResults(), null, schemaName);
                    Table table = tableName != null ? schema.addTable(tableName) : null;
                    Column column = table != null ? table.addColumn(sequences.getString("COLUMN_NAME")) : null;
                    processSequence(inspectionContext, schema, column, statement
                            .executeQuery(createQuery(inspectionContext, schemaName, sequenceName).toString()));
                }
            }
        });
    }

    protected Query createQuery(InspectionContext inspectionContext, String schema, String sequence)
            throws SQLException {
        SelectQuery query = new SelectQuery();
        query.column("*");
        Dialect dialect = inspectionContext.getDialect();
        query.from(dialect.getIdentifier(schema, null) + "." + dialect.getIdentifier(sequence, null));
        return query;
    }

    protected void processSequence(InspectionContext inspectionContext, Schema schema, Column column,
            ResultSet sequences) throws SQLException {
        if (sequences.next()) {
            String name = sequences.getString("SEQUENCE_NAME");
            Sequence sequence = schema.getSequence(name);
            if (sequence == null) {
                schema.addSequence(sequence = new Sequence(name));
            }
            /**
             * Increments the LAST_VALUE by 1, as PostgreSQL LAST_VALUE number
             * points to a value which is already used by a table
             */
            sequence.setLastValue((sequences.getLong("LAST_VALUE") + 1L));
            sequence.setStartWith(sequences.getLong("START_VALUE"));
            sequence.setMinValue(sequences.getLong("MIN_VALUE"));
            sequence.setMaxValue(sequences.getLong("MAX_VALUE"));
            sequence.setIncrementBy(sequences.getLong("INCREMENT_BY"));
            sequence.setCache(sequences.getInt("CACHE_VALUE"));
            sequence.setCycle("T".equalsIgnoreCase(sequences.getString("IS_CYCLED")));
            if (column != null) {
                column.setSequence(sequence);
            }
            inspectionContext.getInspectionResults().addObject(sequence);
        }
    }
}
