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
package com.nuodb.migration.dump;

import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.JdbcServices;
import com.nuodb.migration.jdbc.connection.ConnectionCallback;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.dialect.DatabaseDialect;
import com.nuodb.migration.jdbc.metamodel.Database;
import com.nuodb.migration.jdbc.metamodel.DatabaseInspector;
import com.nuodb.migration.jdbc.metamodel.Table;
import com.nuodb.migration.jdbc.query.*;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import com.nuodb.migration.result.catalog.ResultCatalog;
import com.nuodb.migration.result.catalog.ResultEntry;
import com.nuodb.migration.result.catalog.ResultEntryImpl;
import com.nuodb.migration.result.catalog.ResultEntryWriter;
import com.nuodb.migration.result.format.ResultFormatFactory;
import com.nuodb.migration.result.format.ResultOutput;
import com.nuodb.migration.spec.NativeQuerySpec;
import com.nuodb.migration.spec.SelectQuerySpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static com.google.common.io.Closeables.closeQuietly;
import static com.nuodb.migration.jdbc.metamodel.ObjectType.COLUMN;
import static com.nuodb.migration.jdbc.metamodel.ObjectType.TABLE;
import static com.nuodb.util.StringUtils.isEmpty;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * @author Sergey Bushik
 */
public class DumpJob extends JobBase {

    private static final String QUERY_ENTRY_NAME = "query-%1$tH-%1$tM-%1$tS";

    protected final Log log = LogFactory.getLog(getClass());

    private JdbcServices jdbcServices;
    private ResultCatalog resultCatalog;
    private Collection<SelectQuerySpec> selectQuerySpecs;
    private Collection<NativeQuerySpec> nativeQuerySpecs;
    private String outputType;
    private Map<String, String> outputAttributes;
    private ResultFormatFactory resultFormatFactory;

    @Override
    public void execute(final JobExecution execution) throws Exception {
        ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        connectionProvider.execute(new ConnectionCallback() {
            @Override
            public void execute(Connection connection) throws SQLException {
                DatabaseInspector databaseInspector = jdbcServices.getDatabaseIntrospector();
                databaseInspector.withObjectTypes(TABLE, COLUMN);
                databaseInspector.withConnection(connection);
                Database database = databaseInspector.inspect();
                DatabaseDialect dialect = database.getDatabaseDialect();
                dialect.setTransactionIsolationLevel(connection,
                        new int[]{TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED});

                ResultEntryWriter writer = resultCatalog.openWriter();
                try {
                    for (SelectQuery selectQuery : createSelectQueries(database, selectQuerySpecs)) {
                        OutputStream output = getEntryOutput(writer, createEntry(selectQuery, outputType));
                        dump(execution, connection, database, selectQuery, output);
                    }
                    for (NativeQuery nativeQuery : createNativeQueries(database, nativeQuerySpecs)) {
                        OutputStream output = getEntryOutput(writer, createEntry(nativeQuery, outputType));
                        dump(execution, connection, database, nativeQuery, output);
                    }
                } finally {
                    closeQuietly(writer);
                }
            }
        });
    }

    protected ResultEntry createEntry(SelectQuery selectQuery, String type) {
        Table table = selectQuery.getTables().get(0);
        return new ResultEntryImpl(table.getName(), type);
    }

    protected ResultEntry createEntry(NativeQuery nativeQuery, String type) {
        return new ResultEntryImpl(String.format(QUERY_ENTRY_NAME, new Date()), type);
    }

    protected OutputStream getEntryOutput(ResultEntryWriter writer, ResultEntry entry) {
        writer.addEntry(entry);
        return writer.getEntryOutput(entry);
    }

    protected void dump(JobExecution execution, Connection connection, Database database, Query query,
                        OutputStream output) throws SQLException {
        try {
            ResultOutput resultOutput = resultFormatFactory.createOutput(outputType);
            resultOutput.setAttributes(outputAttributes);
            resultOutput.setJdbcTypeValueAccess(jdbcServices.getJdbcTypeValueAccess());
            resultOutput.setOutputStream(output);
            resultOutput.initOutput();
            dump(execution, connection, database, query, resultOutput);
        } finally {
            closeQuietly(output);
        }
    }

    protected void dump(final JobExecution execution, final Connection connection, final Database database,
                        final Query query, final ResultOutput resultOutput) throws SQLException {
        final DatabaseDialect databaseDialect = database.getDatabaseDialect();
        QueryTemplate queryTemplate = new QueryTemplate(connection);
        queryTemplate.execute(
                new StatementBuilder<PreparedStatement>() {
                    @Override
                    public PreparedStatement build(Connection connection) throws SQLException {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Preparing SQL query %s", query.toQuery()));
                        }
                        PreparedStatement statement = connection.prepareStatement(query.toQuery(),
                                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                        databaseDialect.enableStreaming(statement);
                        return statement;
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        ResultSet resultSet = statement.executeQuery();
                        resultOutput.setResultSet(resultSet);
                        resultOutput.initModel();

                        resultOutput.writeBegin();
                        while (execution.isRunning() && resultSet.next()) {
                            resultOutput.writeRow();
                        }
                        resultOutput.writeEnd();
                    }
                }
        );
    }

    protected Collection<SelectQuery> createSelectQueries(Database database,
                                                          Collection<SelectQuerySpec> selectQuerySpecs) {
        Collection<SelectQuery> selectQueries = Lists.newArrayList();
        if (selectQuerySpecs.isEmpty()) {
            selectQueries.addAll(createSelectQueries(database));
        } else {
            for (SelectQuerySpec selectQuerySpec : selectQuerySpecs) {
                selectQueries.add(createSelectQuery(database, selectQuerySpec));
            }
        }
        return selectQueries;
    }

    protected Collection<SelectQuery> createSelectQueries(Database database) {
        Collection<SelectQuery> selectQueries = Lists.newArrayList();
        for (Table table : database.listTables()) {
            SelectQueryBuilder builder = new SelectQueryBuilder();
            builder.setTable(table);
            builder.setQualifyNames(true);
            selectQueries.add(builder.build());
        }
        return selectQueries;
    }

    protected SelectQuery createSelectQuery(Database database, SelectQuerySpec selectQuerySpec) {
        String tableName = selectQuerySpec.getTable();
        SelectQueryBuilder builder = new SelectQueryBuilder();
        builder.setQualifyNames(true);
        builder.setTable(database.findTable(tableName));
        builder.setColumns(selectQuerySpec.getColumns());
        if (!isEmpty(selectQuerySpec.getFilter())) {
            builder.addFilter(selectQuerySpec.getFilter());
        }
        return builder.build();
    }

    protected Collection<NativeQuery> createNativeQueries(Database database,
                                                          Collection<NativeQuerySpec> nativeQuerySpecs) {
        Collection<NativeQuery> queries = Lists.newArrayList();
        for (NativeQuerySpec nativeQuerySpec : nativeQuerySpecs) {
            NativeQueryBuilder builder = new NativeQueryBuilder();
            builder.setQuery(nativeQuerySpec.getQuery());
            queries.add(builder.build());
        }
        return queries;
    }

    public JdbcServices getJdbcServices() {
        return jdbcServices;
    }

    public void setJdbcServices(JdbcServices jdbcServices) {
        this.jdbcServices = jdbcServices;
    }

    public ResultCatalog getResultCatalog() {
        return resultCatalog;
    }

    public void setResultCatalog(ResultCatalog resultCatalog) {
        this.resultCatalog = resultCatalog;
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

    public ResultFormatFactory getResultFormatFactory() {
        return resultFormatFactory;
    }

    public void setResultFormatFactory(ResultFormatFactory resultFormatFactory) {
        this.resultFormatFactory = resultFormatFactory;
    }
}
