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

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
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
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TABLE;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.query.QueryUtils.AND;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLSequenceInspector extends TableInspectorBase<Table, TableInspectionScope> {

    private static final String QUERY =
            "WITH SEQUENCES AS " +
            "(SELECT N.NSPNAME AS SCHEMA_NAME, C.RELNAME AS TABLE_NAME, A.ATTNAME AS COLUMN_NAME, " +
            "SUBSTRING(PG_GET_EXPR(DEF.ADBIN, DEF.ADRELID) FROM 'nextval\\(''(.*)''::.*\\)') AS SEQUENCE_NAME " +
            "FROM PG_CATALOG.PG_NAMESPACE N " +
            "JOIN PG_CATALOG.PG_CLASS C ON (C.RELNAMESPACE = N.OID) " +
            "JOIN PG_CATALOG.PG_ATTRIBUTE A ON (A.ATTRELID=C.OID) " +
            "LEFT JOIN PG_CATALOG.PG_ATTRDEF DEF ON (A.ATTRELID=DEF.ADRELID AND A.ATTNUM = DEF.ADNUM) " +
            "WHERE A.ATTNUM>0 AND NOT A.ATTISDROPPED AND ADSRC IS NOT NULL AND SUBSTRING(ADSRC FROM 0 FOR 9)" +
            "='nextval(') SELECT * FROM SEQUENCES";

    public PostgreSQLSequenceInspector() {
        super(SEQUENCE, TableInspectionScope.class);
    }

    @Override
    protected Query createQuery(InspectionContext inspectionContext, TableInspectionScope tableInspectionScope) {
        Collection<String> filters = newArrayList();
        Collection<Object> parameters = newArrayList();
        if (tableInspectionScope.getSchema() != null) {
            filters.add("SCHEMA_NAME=?");
            parameters.add(tableInspectionScope.getSchema());
        }
        if (tableInspectionScope.getTable() != null) {
            filters.add("TABLE_NAME=?");
            parameters.add(tableInspectionScope.getTable());
        }
        return new ParameterizedQuery(newQuery(where(QUERY, filters, AND)), parameters);
    }

    @Override
    protected void processResultSet(final InspectionContext inspectionContext, final ResultSet sequences) throws SQLException {
        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.executeStatement(
                new StatementFactory<Statement>() {
                    @Override
                    public Statement createStatement(Connection connection) throws SQLException {
                        return connection.createStatement();
                    }
                }, new StatementCallback<Statement>() {
                    @Override
                    public void executeStatement(Statement statement) throws SQLException {
                        while (sequences.next()) {
                            String schema = sequences.getString("SCHEMA_NAME");
                            String table = sequences.getString("TABLE_NAME");
                            String sequence = sequences.getString("SEQUENCE_NAME");
                            Column column = addTable(inspectionContext.getInspectionResults(), null, schema, table).
                                    addColumn(sequences.getString("COLUMN_NAME"));
                            processSequence(inspectionContext, column, statement.executeQuery(
                                    createQuery(inspectionContext, schema, sequence).toString()));
                        }
                    }
                }
        );
    }

    protected Query createQuery(InspectionContext inspectionContext, String schema, String sequence)
            throws SQLException {
        SelectQuery query = new SelectQuery();
        query.column("*");
        Dialect dialect = inspectionContext.getDialect();
        query.from(dialect.getIdentifier(schema, null) + "." + dialect.getIdentifier(sequence, null));
        return query;
    }

    protected void processSequence(InspectionContext inspectionContext, Column column, ResultSet sequences)
            throws SQLException {
        if (sequences.next()) {
            Sequence sequence = new Sequence();
            sequence.setName(sequences.getString("SEQUENCE_NAME"));
            sequence.setLastValue(sequences.getLong("LAST_VALUE"));
            sequence.setStartWith(sequences.getLong("START_VALUE"));
            sequence.setMinValue(sequences.getLong("MIN_VALUE"));
            sequence.setMaxValue(sequences.getLong("MAX_VALUE"));
            sequence.setIncrementBy(sequences.getLong("INCREMENT_BY"));
            sequence.setCache(sequences.getInt("CACHE_VALUE"));
            sequence.setCycle("T".equalsIgnoreCase(sequences.getString("IS_CYCLED")));
            column.setSequence(sequence);
            column.getTable().getSchema().addSequence(sequence);
            inspectionContext.getInspectionResults().addObject(sequence);
        }
    }
}
