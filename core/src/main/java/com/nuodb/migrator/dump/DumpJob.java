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
package com.nuodb.migrator.dump;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.connection.ConnectionServices;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.model.ValueModel;
import com.nuodb.migrator.jdbc.model.ValueModelFactory;
import com.nuodb.migrator.jdbc.model.ValueModelList;
import com.nuodb.migrator.jdbc.query.*;
import com.nuodb.migrator.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migrator.jdbc.type.access.JdbcTypeValueAccessProvider;
import com.nuodb.migrator.job.JobBase;
import com.nuodb.migrator.job.JobExecution;
import com.nuodb.migrator.resultset.catalog.Catalog;
import com.nuodb.migrator.resultset.catalog.CatalogEntry;
import com.nuodb.migrator.resultset.catalog.CatalogWriter;
import com.nuodb.migrator.resultset.format.FormatOutput;
import com.nuodb.migrator.resultset.format.FormatFactory;
import com.nuodb.migrator.resultset.format.value.SimpleValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.spec.NativeQuerySpec;
import com.nuodb.migrator.spec.SelectQuerySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;

import static com.google.common.collect.Iterables.get;
import static com.google.common.io.Closeables.closeQuietly;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.Table.TABLE;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;
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

    private static final String QUERY_ENTRY_NAME = "query-%1$tH%1$tM%1$tS";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private TimeZone timeZone;
    private Catalog catalog;
    private Collection<String> tableTypes = Collections.singleton(TABLE);
    private Collection<SelectQuerySpec> selectQuerySpecs;
    private Collection<NativeQuerySpec> nativeQuerySpecs;
    private String outputType;
    private Map<String, Object> attributes;
    private ConnectionProvider connectionProvider;
    private DialectResolver dialectResolver;
    private FormatFactory formatFactory;
    private ValueFormatRegistryResolver valueFormatRegistryResolver;

    @Override
    public void execute(JobExecution execution) throws Exception {
        validate();
        execute(new DumpJobExecution(execution));
    }

    protected void validate() {
        isNotNull(getCatalog(), "Catalog is required");
        isNotNull(getConnectionProvider(), "Connection provider is required");
        isNotNull(getDialectResolver(), "Dialect resolver is required");
        isNotNull(getValueFormatRegistryResolver(), "Value format registry resolver is required");
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

        InspectionManager inspectionManager = new InspectionManager();
        inspectionManager.setDialectResolver(getDialectResolver());
        inspectionManager.setConnection(connectionServices.getConnection());
        Database database = inspectionManager.inspect(
                new TableInspectionScope(connectionServices.getCatalog(), connectionServices.getSchema()),
                MetaDataType.DATABASE, MetaDataType.CATALOG, MetaDataType.SCHEMA,
                MetaDataType.TABLE, MetaDataType.COLUMN
        ).getObject(MetaDataType.DATABASE);
        execution.setDatabase(database);

        Connection connection = connectionServices.getConnection();
        execution.setValueFormatRegistry(getValueFormatRegistryResolver().resolve(connection));

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
                dump(execution, selectQuery);
            }
            for (NativeQuery nativeQuery : createNativeQueries(getNativeQuerySpecs())) {
                dump(execution, nativeQuery);
            }
            connection.commit();
        } finally {
            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, null);
            }
            closeQuietly(catalogWriter);
        }
    }

    protected void dump(final DumpJobExecution execution, final Query query) throws SQLException {
        final Database database = execution.getDatabase();
        StatementTemplate template = new StatementTemplate(execution.getConnectionServices().getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        PreparedStatement statement = connection.prepareStatement(
                                query.toQuery(), TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                        database.getDialect().setStreamResults(statement, true);
                        return statement;
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement preparedStatement) throws SQLException {
                        dump(execution, query, preparedStatement);
                    }
                }
        );
    }

    protected void dump(DumpJobExecution execution, Query query,
                        PreparedStatement preparedStatement) throws SQLException {
        final FormatOutput formatOutput = getFormatFactory().createOutput(getOutputType());
        formatOutput.setAttributes(getAttributes());
        formatOutput.setValueFormatRegistry(execution.getValueFormatRegistry());

        Dialect dialect = execution.getDatabase().getDialect();
        if (!dialect.supportsSessionTimeZone() && dialect.supportsStatementWithTimezone()) {
            formatOutput.setTimeZone(getTimeZone());
        }
        JdbcTypeRegistry jdbcTypeRegistry = dialect.getJdbcTypeRegistry();
        formatOutput.setValueAccessProvider(new JdbcTypeValueAccessProvider(jdbcTypeRegistry));

        ResultSet resultSet = preparedStatement.executeQuery();
        formatOutput.setResultSet(resultSet);
        formatOutput.setValueFormatModelList(createValueModelList(resultSet, query));
        formatOutput.initValueFormatModel();

        CatalogWriter catalogWriter = execution.getCatalogWriter();
        CatalogEntry catalogEntry = createCatalogEntry(query);
        catalogWriter.write(catalogEntry);
        formatOutput.setOutputStream(catalogWriter.getOutputStream(catalogEntry));
        formatOutput.initOutput();

        formatOutput.writeBegin();
        while (execution.isRunning() && formatOutput.hasNextRow()) {
            formatOutput.writeRow();
        }
        formatOutput.writeEnd();
    }

    protected CatalogEntry createCatalogEntry(Query query) {
        if (query instanceof SelectQuery) {
            Table table = get(((SelectQuery) query).getTables(), 0);
            return new CatalogEntry(table.getName().toLowerCase(), getOutputType());
        } else {
            return new CatalogEntry(format(QUERY_ENTRY_NAME, new Date()), getOutputType());
        }
    }

    protected ValueModelList<ValueFormatModel> createValueModelList(ResultSet resultSet, Query query) throws SQLException {
        Collection<? extends ValueModel> valueModelList = query instanceof SelectQuery ?
                ((SelectQuery) query).getColumns() : ValueModelFactory.createValueModelList(resultSet);
        return ValueModelFactory.createValueModelList(
                Iterables.transform(valueModelList,
                    new Function<ValueModel, ValueFormatModel>() {
                        @Override
                        public ValueFormatModel apply(ValueModel valueModel) {
                            return new SimpleValueFormatModel(valueModel);
                        }
                    }));
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
        for (Table table : database.getTables()) {
            if (getTableTypes().contains(table.getType())) {
                SelectQueryBuilder builder = new SelectQueryBuilder();
                builder.setDialect(dialect);
                builder.setTable(table);
                builder.setQualifyNames(true);
                selectQueries.add(builder.build());
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(format("Skip table %s type %s", table.getQualifiedName(dialect), table.getType()));
                }
            }
        }
        return selectQueries;
    }

    protected SelectQuery createSelectQuery(Database database, SelectQuerySpec selectQuerySpec) {
        SelectQueryBuilder builder = new SelectQueryBuilder();
        String tableName = selectQuerySpec.getTable();
        builder.setQualifyNames(true);
        builder.setDialect(database.getDialect());
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

    public Collection<String> getTableTypes() {
        return tableTypes;
    }

    public void setTableTypes(Collection<String> tableTypes) {
        this.tableTypes = tableTypes;
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    public ValueFormatRegistryResolver getValueFormatRegistryResolver() {
        return valueFormatRegistryResolver;
    }

    public void setValueFormatRegistryResolver(
            ValueFormatRegistryResolver valueFormatRegistryResolver) {
        this.valueFormatRegistryResolver = valueFormatRegistryResolver;
    }
}
