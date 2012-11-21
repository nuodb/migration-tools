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
import com.nuodb.migration.jdbc.ConnectionServices;
import com.nuodb.migration.jdbc.ConnectionServicesCallback;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.model.Database;
import com.nuodb.migration.jdbc.model.DatabaseInspector;
import com.nuodb.migration.jdbc.model.Table;
import com.nuodb.migration.jdbc.query.*;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessProvider;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import com.nuodb.migration.resultset.catalog.Catalog;
import com.nuodb.migration.resultset.catalog.CatalogEntry;
import com.nuodb.migration.resultset.catalog.CatalogWriter;
import com.nuodb.migration.resultset.format.ResultSetFormatFactory;
import com.nuodb.migration.resultset.format.ResultSetOutput;
import com.nuodb.migration.spec.NativeQuerySpec;
import com.nuodb.migration.spec.SelectQuerySpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.io.Closeables.closeQuietly;
import static com.nuodb.migration.jdbc.ConnectionServicesFactory.createConnectionServices;
import static com.nuodb.migration.jdbc.model.ObjectType.*;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class DumpJob extends JobBase {

    private static final String QUERY_ENTRY_NAME = "query-%1$tH-%1$tM-%1$tS";

    protected final Log log = LogFactory.getLog(getClass());

    private ConnectionProvider connectionProvider;
    private TimeZone timeZone;
    private Catalog catalog;
    private Collection<SelectQuerySpec> selectQuerySpecs;
    private Collection<NativeQuerySpec> nativeQuerySpecs;
    private String outputType;
    private Map<String, String> attributes;
    private ResultSetFormatFactory resultSetFormatFactory;

    @Override
    public void execute(final JobExecution execution) throws Exception {
        ConnectionServicesCallback.execute(createConnectionServices(getConnectionProvider()),
                new ConnectionServicesCallback() {
                    @Override
                    public void execute(ConnectionServices services) throws SQLException {
                        dump(execution, services);
                    }
                });
    }

    protected void dump(final JobExecution execution, final ConnectionServices services) throws SQLException {
        DatabaseInspector databaseInspector = services.getDatabaseInspector();
        databaseInspector.withObjectTypes(CATALOG, SCHEMA, TABLE, COLUMN);
        Database database = databaseInspector.inspect();

        Dialect dialect = database.getDialect();
        dialect.setSupportedTransactionIsolation(services.getConnection(),
                new int[]{TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED});
        CatalogWriter writer = getCatalog().getWriter();
        try {
            for (SelectQuery selectQuery : createSelectQueries(database, getSelectQuerySpecs())) {
                dump(execution, services, database, selectQuery, writer, createEntry(selectQuery, getOutputType()));
            }
            for (NativeQuery nativeQuery : createNativeQueries(database, getNativeQuerySpecs())) {
                dump(execution, services, database, nativeQuery, writer, createEntry(nativeQuery, getOutputType()));
            }
        } finally {
            closeQuietly(writer);
        }
    }

    protected CatalogEntry createEntry(SelectQuery query, String type) {
        Table table = query.getTables().get(0);
        return new CatalogEntry(table.getName(), type);
    }

    protected CatalogEntry createEntry(NativeQuery query, String type) {
        return new CatalogEntry(String.format(QUERY_ENTRY_NAME, new Date()), type);
    }

    protected void dump(final JobExecution execution, final ConnectionServices connectionServices,
                        final Database database, final Query query, final CatalogWriter writer,
                        final CatalogEntry entry) throws SQLException {
        final ResultSetOutput resultSetOutput = getResultSetFormatFactory().createOutput(getOutputType());
        resultSetOutput.setAttributes(getAttributes());
        resultSetOutput.setTimeZone(getTimeZone());
        resultSetOutput.setJdbcTypeValueAccessProvider(new JdbcTypeValueAccessProvider(
                database.getDialect().getJdbcTypeRegistry()));

        QueryTemplate queryTemplate = new QueryTemplate(connectionServices.getConnection());
        queryTemplate.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return createStatement(connection, database, query);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement statement) throws SQLException {
                        dump(execution, statement, writer, entry, resultSetOutput);
                    }
                }
        );
    }

    protected PreparedStatement createStatement(final Connection connection, final Database database,
                                                final Query query) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Preparing SQL query %s", query.toQuery()));
        }
        PreparedStatement statement = connection.prepareStatement(query.toQuery(), TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        Dialect dialect = database.getDialect();
        dialect.enableStreaming(statement);
        return statement;
    }

    protected void dump(final JobExecution execution, final PreparedStatement statement, final CatalogWriter writer,
                        final CatalogEntry entry, final ResultSetOutput resultSetOutput) throws SQLException {
        ResultSet resultSet = statement.executeQuery();

        writer.addEntry(entry);
        resultSetOutput.setOutputStream(writer.getEntryOutput(entry));
        resultSetOutput.setResultSet(resultSet);

        resultSetOutput.writeBegin();
        while (execution.isRunning() && resultSet.next()) {
            resultSetOutput.writeRow();
        }
        resultSetOutput.writeEnd();
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
            if (Table.TABLE.equals(table.getType())) {
                SelectQueryBuilder builder = new SelectQueryBuilder();
                builder.setTable(table);
                builder.setQualifyNames(true);
                selectQueries.add(builder.build());
            } else {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Skipping %s %s", table.getQualifiedName(), table.getType()));
                }
            }
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

    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    public void setConnectionProvider(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
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

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public ResultSetFormatFactory getResultSetFormatFactory() {
        return resultSetFormatFactory;
    }

    public void setResultSetFormatFactory(ResultSetFormatFactory resultSetFormatFactory) {
        this.resultSetFormatFactory = resultSetFormatFactory;
    }
}
