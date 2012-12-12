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
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.ConnectionServices;
import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.dialect.DialectResolver;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.Table;
import com.nuodb.migration.jdbc.metadata.inspector.DatabaseInspector;
import com.nuodb.migration.jdbc.query.*;
import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessProvider;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import com.nuodb.migration.resultset.catalog.Catalog;
import com.nuodb.migration.resultset.catalog.CatalogEntry;
import com.nuodb.migration.resultset.catalog.CatalogWriter;
import com.nuodb.migration.resultset.format.ResultSetFormatFactory;
import com.nuodb.migration.resultset.format.ResultSetOutput;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormatRegistryResolver;
import com.nuodb.migration.spec.NativeQuerySpec;
import com.nuodb.migration.spec.SelectQuerySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.io.Closeables.closeQuietly;
import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migration.utils.ValidationUtils.isNotNull;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class DumpJob extends JobBase {

    private static final String QUERY_ENTRY_NAME = "query-%1$tH-%1$tM-%1$tS";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private TimeZone timeZone;
    private Catalog catalog;
    private Collection<SelectQuerySpec> selectQuerySpecs;
    private Collection<NativeQuerySpec> nativeQuerySpecs;
    private String outputType;
    private Map<String, String> attributes;
    private ConnectionProvider connectionProvider;
    private DialectResolver dialectResolver;
    private ResultSetFormatFactory resultSetFormatFactory;
    private JdbcTypeValueFormatRegistryResolver jdbcTypeValueFormatRegistryResolver;

    @Override
    public void execute(JobExecution execution) throws Exception {
        validate();
        execute(new DumpJobExecution(execution));
    }

    protected void validate() {
        isNotNull(getCatalog(), "Catalog is required");
        isNotNull(getConnectionProvider(), "Connection provider is required");
        isNotNull(getDialectResolver(), "Database dialect resolver is required");
        isNotNull(getJdbcTypeValueFormatRegistryResolver(), "JDBC type value format registry resolver is required");
    }

    protected void execute(DumpJobExecution execution) throws SQLException {
        ConnectionServices connectionServices = getConnectionProvider().getConnectionServices();
        try {
            execution.setConnectionServices(connectionServices);
            dump(execution);
        } finally {
            close(connectionServices);
        }
    }

    protected void dump(DumpJobExecution execution) throws SQLException {
        ConnectionServices connectionServices = execution.getConnectionServices();

        DatabaseInspector databaseInspector = connectionServices.createDatabaseInspector();
        databaseInspector.withMetaDataTypes(CATALOG, SCHEMA, TABLE, COLUMN);
        databaseInspector.withDialectResolver(getDialectResolver());

        Connection connection = connectionServices.getConnection();
        Database database = databaseInspector.inspect();
        execution.setDatabase(database);

        DatabaseMetaData metaData = connection.getMetaData();
        execution.setJdbcTypeValueFormatRegistry(getJdbcTypeValueFormatRegistryResolver().resolveObject(metaData));

        CatalogWriter catalogWriter = getCatalog().getCatalogWriter();
        execution.setCatalogWriter(catalogWriter);

        Dialect dialect = database.getDialect();
        try {
            dialect.setTransactionIsolationLevel(connection,
                    new int[]{TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED});

            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, timeZone);
            }

            for (SelectQuery selectQuery : createSelectQueries(database, getSelectQuerySpecs())) {
                dump(execution, selectQuery, createCatalogEntry(selectQuery, getOutputType()));
            }
            for (NativeQuery nativeQuery : createNativeQueries(getNativeQuerySpecs())) {
                dump(execution, nativeQuery, createCatalogEntry(nativeQuery, getOutputType()));
            }
            connection.commit();
        } finally {
            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, null);
            }
            closeQuietly(catalogWriter);
        }
    }

    protected CatalogEntry createCatalogEntry(SelectQuery selectQuery, String type) {
        Table table = selectQuery.getTables().get(0);
        return new CatalogEntry(table.getName(), type);
    }

    protected CatalogEntry createCatalogEntry(NativeQuery nativeQuery, String type) {
        return new CatalogEntry(format(QUERY_ENTRY_NAME, new Date()), type);
    }

    protected void dump(final DumpJobExecution execution, final Query query,
                        final CatalogEntry catalogEntry) throws SQLException {
        final Database database = execution.getDatabase();
        StatementTemplate template = new StatementTemplate(execution.getConnectionServices().getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return prepareStatement(connection, database, query);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement preparedStatement) throws SQLException {
                        dump(execution, preparedStatement, catalogEntry);
                    }
                }
        );
    }

    protected void dump(DumpJobExecution execution, PreparedStatement preparedStatement,
                        CatalogEntry catalogEntry) throws SQLException {
        final ResultSetOutput resultSetOutput = getResultSetFormatFactory().createOutput(getOutputType());
        resultSetOutput.setAttributes(getAttributes());
        resultSetOutput.setJdbcTypeValueFormatRegistry(execution.getJdbcTypeValueFormatRegistry());

        Dialect dialect = execution.getDatabase().getDialect();
        if (!dialect.supportsSessionTimeZone() && dialect.supportsStatementWithTimezone()) {
            resultSetOutput.setTimeZone(getTimeZone());
        }
        JdbcTypeRegistry jdbcTypeRegistry = dialect.getJdbcTypeRegistry();
        resultSetOutput.setJdbcTypeValueAccessProvider(new JdbcTypeValueAccessProvider(jdbcTypeRegistry));

        ResultSet resultSet = preparedStatement.executeQuery();

        CatalogWriter catalogWriter = execution.getCatalogWriter();
        catalogWriter.addEntry(catalogEntry);
        resultSetOutput.setOutputStream(catalogWriter.getEntryOutput(catalogEntry));
        resultSetOutput.setResultSet(resultSet);

        resultSetOutput.writeBegin();
        while (execution.isRunning() && resultSet.next()) {
            resultSetOutput.writeRow();
        }
        resultSetOutput.writeEnd();
    }

    protected PreparedStatement prepareStatement(Connection connection, Database database,
                                                 Query query) throws SQLException {
        if (logger.isDebugEnabled()) {
            logger.debug(query.toQuery());
        }
        PreparedStatement preparedStatement = connection.prepareStatement(
                query.toQuery(), TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        Dialect dialect = database.getDialect();
        dialect.stream(preparedStatement);
        return preparedStatement;
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
        Dialect dialect = database.getDialect();
        Collection<SelectQuery> selectQueries = Lists.newArrayList();
        for (Table table : database.listTables()) {
            if (Table.TABLE.equals(table.getType())) {
                SelectQueryBuilder builder = new SelectQueryBuilder();
                builder.setDialect(dialect);
                builder.setTable(table);
                builder.setQualifyNames(true);
                selectQueries.add(builder.build());
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(format("Skip %s %s", table.getQualifiedName(dialect), table.getType()));
                }
            }
        }
        return selectQueries;
    }

    protected SelectQuery createSelectQuery(Database database, SelectQuerySpec selectQuerySpec) {
        Dialect dialect = database.getDialect();
        String tableName = selectQuerySpec.getTable();
        SelectQueryBuilder builder = new SelectQueryBuilder();
        builder.setQualifyNames(true);
        builder.setDialect(dialect);
        builder.setTable(database.findTable(tableName));
        builder.setColumns(selectQuerySpec.getColumns());
        if (!isEmpty(selectQuerySpec.getFilter())) {
            builder.addFilter(selectQuerySpec.getFilter());
        }
        return builder.build();
    }

    protected Collection<NativeQuery> createNativeQueries(Collection<NativeQuerySpec> nativeQuerySpecs) {
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

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    public ResultSetFormatFactory getResultSetFormatFactory() {
        return resultSetFormatFactory;
    }

    public void setResultSetFormatFactory(ResultSetFormatFactory resultSetFormatFactory) {
        this.resultSetFormatFactory = resultSetFormatFactory;
    }

    public JdbcTypeValueFormatRegistryResolver getJdbcTypeValueFormatRegistryResolver() {
        return jdbcTypeValueFormatRegistryResolver;
    }

    public void setJdbcTypeValueFormatRegistryResolver(
            JdbcTypeValueFormatRegistryResolver jdbcTypeValueFormatRegistryResolver) {
        this.jdbcTypeValueFormatRegistryResolver = jdbcTypeValueFormatRegistryResolver;
    }
}
