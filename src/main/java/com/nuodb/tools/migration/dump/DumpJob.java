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
package com.nuodb.tools.migration.dump;

import com.nuodb.tools.migration.dump.output.OutputFormat;
import com.nuodb.tools.migration.dump.output.OutputFormatFactory;
import com.nuodb.tools.migration.format.catalog.*;
import com.nuodb.tools.migration.format.catalog.entry.NativeQueryEntry;
import com.nuodb.tools.migration.format.catalog.entry.SelectQueryEntry;
import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.connection.ConnectionCallback;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.Database;
import com.nuodb.tools.migration.jdbc.metamodel.DatabaseIntrospector;
import com.nuodb.tools.migration.jdbc.metamodel.Table;
import com.nuodb.tools.migration.jdbc.query.*;
import com.nuodb.tools.migration.job.JobBase;
import com.nuodb.tools.migration.job.JobExecution;
import com.nuodb.tools.migration.spec.NativeQuerySpec;
import com.nuodb.tools.migration.spec.SelectQuerySpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static com.nuodb.tools.migration.jdbc.metamodel.ObjectType.*;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class DumpJob extends JobBase {

    protected final Log log = LogFactory.getLog(getClass());

    private JdbcServices jdbcServices;
    private QueryEntryCatalog queryEntryCatalog;
    private Collection<SelectQuerySpec> selectQuerySpecs;
    private Collection<NativeQuerySpec> nativeQuerySpecs;
    private String outputType;
    private Map<String, String> outputAttributes;
    private OutputFormatFactory outputFormatFactory;

    @Override
    public void execute(final JobExecution execution) throws Exception {
        ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        connectionProvider.execute(new ConnectionCallback() {
            @Override
            public void execute(Connection connection) throws SQLException {
                DatabaseIntrospector databaseIntrospector = jdbcServices.getDatabaseIntrospector();
                databaseIntrospector.withObjectTypes(CATALOG, SCHEMA, TABLE, COLUMN);
                databaseIntrospector.withConnection(connection);
                Database database = databaseIntrospector.introspect();

                QueryEntryWriter writer = queryEntryCatalog.openQueryEntryWriter();
                try {
                    for (SelectQuery selectQuery : createSelectQueries(database, selectQuerySpecs)) {
                        dump(execution, connection, writer, new SelectQueryEntry(selectQuery));
                    }
                    for (NativeQuery nativeQuery : createNativeQueries(database, nativeQuerySpecs)) {
                        dump(execution, connection, writer, new NativeQueryEntry(nativeQuery));
                    }
                } finally {
                    writer.close();
                }
            }
        });
    }

    protected void dump(JobExecution execution, Connection connection, QueryEntryWriter writer,
                        QueryEntry entry) throws SQLException {
        OutputFormat output = outputFormatFactory.createOutputFormat(outputType);
        output.setAttributes(outputAttributes);
        output.setJdbcTypeExtractor(jdbcServices.getJdbcTypeExtractor());
        output.setOutputStream(writer.write(entry));
        try {
            dump(execution, connection, entry.getQuery(), output);
        } finally {
            writer.close(entry);
        }
    }

    protected void dump(final JobExecution execution, final Connection connection, final Query query,
                        final OutputFormat output) throws SQLException {
        QueryTemplate queryTemplate = new QueryTemplate(connection);
        queryTemplate.execute(
                new StatementBuilder<PreparedStatement>() {
                    @Override
                    public PreparedStatement build(Connection connection) throws SQLException {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Preparing SQL query %1$s", query.toQuery()));
                        }
                        return connection.prepareStatement(query.toQuery(), TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Writing dump with %1$s", output.getClass().getName()));
                        }
                        ResultSet resultSet = statement.executeQuery();
                        output.outputBegin(resultSet);
                        while (execution.isRunning() && resultSet.next()) {
                            output.outputRow(resultSet);
                        }
                        output.outputEnd(resultSet);
                    }
                }
        );
    }

    protected Collection<SelectQuery> createSelectQueries(Database database,
                                                          Collection<SelectQuerySpec> selectQuerySpecs) {
        Collection<SelectQuery> selectQueries = new ArrayList<SelectQuery>();
        Collection<Table> tables = database.listTables();
        if (selectQuerySpecs.isEmpty()) {
            selectQueries.addAll(createSelectQueries(tables));
        } else {
            for (SelectQuerySpec selectQuerySpec : selectQuerySpecs) {
                selectQueries.addAll(createSelectQueries(tables, selectQuerySpec));
            }
        }
        return selectQueries;
    }

    protected Collection<SelectQuery> createSelectQueries(Collection<Table> tables) {
        Collection<SelectQuery> selectQueries = new ArrayList<SelectQuery>();
        for (Table table : tables) {
            SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
            selectQueryBuilder.setQualifyNames(true);
            selectQueryBuilder.setTable(table);
            selectQueries.add(selectQueryBuilder.build());
        }
        return selectQueries;
    }

    protected Collection<SelectQuery> createSelectQueries(Collection<Table> tables, SelectQuerySpec selectQuerySpec) {
        Collection<SelectQuery> selectQueries = new ArrayList<SelectQuery>();
        String tableName = selectQuerySpec.getTable();
        for (Table table : tables) {
            if (tableName.equals(table.getName())) {
                SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
                selectQueryBuilder.setQualifyNames(true);
                selectQueryBuilder.setColumns(selectQuerySpec.getColumns());
                selectQueryBuilder.setTable(table);
                selectQueryBuilder.addFilter(selectQuerySpec.getFilter());
                selectQueries.add(selectQueryBuilder.build());
            }
        }
        return selectQueries;
    }

    protected Collection<NativeQuery> createNativeQueries(Database database,
                                                          Collection<NativeQuerySpec> nativeQuerySpecs) {
        Collection<NativeQuery> queries = new ArrayList<NativeQuery>();
        if (nativeQuerySpecs != null) {
            for (NativeQuerySpec nativeQuerySpec : nativeQuerySpecs) {
                NativeQueryBuilder nativeQueryBuilder = new NativeQueryBuilder();
                nativeQueryBuilder.setQuery(nativeQuerySpec.getQuery());
                queries.add(nativeQueryBuilder.build());
            }
        }
        return queries;
    }

    public JdbcServices getJdbcServices() {
        return jdbcServices;
    }

    public void setJdbcServices(JdbcServices jdbcServices) {
        this.jdbcServices = jdbcServices;
    }

    public QueryEntryCatalog getQueryEntryCatalog() {
        return queryEntryCatalog;
    }

    public void setQueryEntryCatalog(QueryEntryCatalog queryEntryCatalog) {
        this.queryEntryCatalog = queryEntryCatalog;
    }

    public Collection<SelectQuerySpec> getSelectQuerySpecs() {
        return selectQuerySpecs;
    }

    public void setSelectQuerySpecs(Collection<SelectQuerySpec> selectQuerySpecs) {
        this.selectQuerySpecs = selectQuerySpecs;
    }

    public Collection<NativeQuerySpec> getNativeQuerySpecs() {
        return nativeQuerySpecs;
    }

    public void setNativeQuerySpecs(Collection<NativeQuerySpec> nativeQuerySpecs) {
        this.nativeQuerySpecs = nativeQuerySpecs;
    }

    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    public Map<String, String> getOutputAttributes() {
        return outputAttributes;
    }

    public void setOutputAttributes(Map<String, String> outputAttributes) {
        this.outputAttributes = outputAttributes;
    }

    public OutputFormatFactory getOutputFormatFactory() {
        return outputFormatFactory;
    }

    public void setOutputFormatFactory(OutputFormatFactory outputFormatFactory) {
        this.outputFormatFactory = outputFormatFactory;
    }
}
