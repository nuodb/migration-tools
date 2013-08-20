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
package com.nuodb.migrator.load;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.catalog.*;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.InputFormat;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.backup.format.value.ValueHandleList;
import com.nuodb.migrator.jdbc.connection.ConnectionProviderFactory;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.query.*;
import com.nuodb.migrator.job.JobBase;
import com.nuodb.migrator.job.JobExecution;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.LoadSpec;
import com.nuodb.migrator.spec.ResourceSpec;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.format.value.ValueHandleListBuilder.newBuilder;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migrator.utils.CollectionUtils.isEmpty;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class LoadJob extends JobBase {

    protected final Logger logger = getLogger(getClass());

    private LoadSpec loadSpec;
    private FormatFactory formatFactory;
    private DialectResolver dialectResolver;
    private InspectionManager inspectionManager;
    private ConnectionProviderFactory connectionProviderFactory;
    private ValueFormatRegistryResolver valueFormatRegistryResolver;

    private LoadContext loadContext;
    private RowSetMapper rowSetMapper;

    public LoadJob(LoadSpec loadSpec) {
        this.loadSpec = loadSpec;
    }

    @Override
    public void init(JobExecution execution) throws Exception {
        isNotNull(getLoadSpec(), "Load spec is required");
        isNotNull(getFormatFactory(), "Backup format factory is required");
        isNotNull(getDialectResolver(), "Dialect resolver is required");
        isNotNull(getConnectionProviderFactory(), "Connection provider factory is required");
        isNotNull(getValueFormatRegistryResolver(), "Value format registry resolver is required");

        init();
    }

    protected void init() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing load job context");
        }
        loadContext = new LoadContext();
        final ResourceSpec inputSpec = getInputSpec();
        loadContext.setFormatAttributes(inputSpec.getAttributes());
        loadContext.setCatalogManager(new XmlCatalogManager(inputSpec.getPath()));

        final Connection connection = getConnectionProviderFactory().createConnectionProvider(
                getConnectionSpec()).getConnection();
        loadContext.setConnection(connection);
        loadContext.setFormatFactory(getFormatFactory());
        loadContext.setDialect(getDialectResolver().resolve(connection));
        loadContext.setValueFormatRegistry(getValueFormatRegistryResolver().resolve(connection));

        rowSetMapper = new LoadRowSetMapper(loadContext);
    }

    @Override
    public void execute(JobExecution execution) throws Exception {
        load();
    }

    protected void load() throws SQLException {
        final Connection connection = loadContext.getConnection();
        final Dialect dialect = loadContext.getDialect();
        try {
            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, getTimeZone());
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Inspecting target database");
            }
            InspectionScope inspectionScope = new TableInspectionScope(
                    getConnectionSpec().getCatalog(), getConnectionSpec().getSchema());
            Database database = getInspectionManager().inspect(connection, inspectionScope,
                    DATABASE, CATALOG, SCHEMA, TABLE, COLUMN).getObject(DATABASE);
            loadContext.setDatabase(database);

            Catalog catalog = loadContext.getCatalogManager().readCatalog();
            for (RowSet rowSet : catalog.getRowSets()) {
                load(rowSet);
            }
            connection.commit();
        } catch (MigratorException exception) {
            connection.rollback();
            throw exception;
        } catch (Exception exception) {
            connection.rollback();
            throw new LoadException(exception);
        } finally {
            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, null);
            }
        }
    }

    protected void load(final RowSet rowSet) throws SQLException {
        if (!isEmpty(rowSet.getChunks())) {
            final Table table = rowSetMapper.getTable(rowSet);
            if (table != null) {
                final InsertQuery query = createInsertQuery(table, rowSet.getColumns());
                final StatementTemplate template = new StatementTemplate(loadContext.getConnection());
                template.execute(
                        new StatementFactory<PreparedStatement>() {
                            @Override
                            public PreparedStatement create(Connection connection) throws SQLException {
                                return connection.prepareStatement(query.toString());
                            }
                        },
                        new StatementCallback<PreparedStatement>() {
                            @Override
                            public void process(PreparedStatement statement) throws SQLException {
                                load(rowSet, table, statement);
                            }
                        }
                );
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Row set %s is empty, skipping it", rowSet.getName()));
            }
        }
    }

    protected void load(RowSet rowSet, Table table, PreparedStatement statement) throws SQLException {
        ValueHandleList valueHandleList = createValueHandleList(rowSet, table, statement);
        for (Chunk chunk : rowSet.getChunks()) {
            InputFormat inputFormat = getFormatFactory().createInputFormat(
                    rowSet.getCatalog().getFormat(), loadContext.getFormatAttributes());
            inputFormat.setValueHandleList(valueHandleList);
            inputFormat.setInputStream(loadContext.getCatalogManager().openInputStream(chunk.getName()));
            inputFormat.open();
            if (logger.isTraceEnabled()) {
                logger.trace(format("Loading %d rows from %s chunk to %s table",
                        chunk.getRowCount(), chunk.getName(), table.getQualifiedName(null)));
            }
            inputFormat.readStart();
            long row = 0;
            while (inputFormat.setValues()) {
                try {
                    statement.execute();
                } catch (Exception exception) {
                    throw new LoadException(format("Error loading row %d from %s chunk to %s table",
                            row + 1, chunk.getName(), table.getQualifiedName(null)), exception);
                }
                row++;
            }
            inputFormat.readEnd();
            inputFormat.close();
            if (logger.isTraceEnabled()) {
                logger.trace(format("Chunk %s loaded", chunk.getName()));
            }
        }
    }

    protected ValueHandleList createValueHandleList(final RowSet rowSet, Table table,
                                                    PreparedStatement statement) throws SQLException {
        Iterable<com.nuodb.migrator.jdbc.metadata.Column> columns = filter(table.getColumns(),
                new Predicate<com.nuodb.migrator.jdbc.metadata.Column>() {
                    @Override
                    public boolean apply(final com.nuodb.migrator.jdbc.metadata.Column column) {
                        return any(rowSet.getColumns(), new Predicate<Column>() {
                            @Override
                            public boolean apply(Column rowSetColumn) {
                                return column.getName().equalsIgnoreCase(rowSetColumn.getName());
                            }
                        });
                    }
                });
        return newBuilder(statement).
                withColumns(newArrayList(columns)).
                withDialect(loadContext.getDialect()).
                withTimeZone(getTimeZone()).
                withValueFormatRegistry(loadContext.getValueFormatRegistry()).build();
    }

    protected InsertQuery createInsertQuery(Table table, Collection<Column> columns) {
        InsertQueryBuilder builder = new InsertQueryBuilder().insertType(getInsertType(table)).into(table);
        builder.columns(Lists.<String>newArrayList(transform(columns, new Function<Column, String>() {
            @Override
            public String apply(Column column) {
                return column.getName();
            }
        })));
        return builder.build();
    }

    protected InsertType getInsertType(Table table) {
        Database database = table.getDatabase();
        Map<String, InsertType> tableInsertTypes = getTableInsertTypes();
        InsertType insertType = getInsertType();
        if (tableInsertTypes != null) {
            for (Map.Entry<String, InsertType> entry : tableInsertTypes.entrySet()) {
                final Collection<Table> tables = database.findTables(entry.getKey());
                if (tables.contains(table)) {
                    insertType = entry.getValue();
                    break;
                }
            }
        }
        return insertType;
    }

    @Override
    public void release(JobExecution execution) throws Exception {
        close(loadContext.getConnection());
    }

    public LoadSpec getLoadSpec() {
        return loadSpec;
    }

    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    public InspectionManager getInspectionManager() {
        return inspectionManager;
    }

    public void setInspectionManager(InspectionManager inspectionManager) {
        this.inspectionManager = inspectionManager;
    }

    public RowSetMapper getRowSetMapper() {
        return rowSetMapper;
    }

    public void setRowSetMapper(RowSetMapper rowSetMapper) {
        this.rowSetMapper = rowSetMapper;
    }

    public ConnectionProviderFactory getConnectionProviderFactory() {
        return connectionProviderFactory;
    }

    public void setConnectionProviderFactory(ConnectionProviderFactory connectionProviderFactory) {
        this.connectionProviderFactory = connectionProviderFactory;
    }

    public ValueFormatRegistryResolver getValueFormatRegistryResolver() {
        return valueFormatRegistryResolver;
    }

    public void setValueFormatRegistryResolver(ValueFormatRegistryResolver valueFormatRegistryResolver) {
        this.valueFormatRegistryResolver = valueFormatRegistryResolver;
    }

    protected Map<String, InsertType> getTableInsertTypes() {
        return loadSpec.getTableInsertTypes();
    }

    protected ResourceSpec getInputSpec() {
        return loadSpec.getInputSpec();
    }

    protected InsertType getInsertType() {
        return loadSpec.getInsertType();
    }

    protected TimeZone getTimeZone() {
        return loadSpec.getTimeZone();
    }

    protected ConnectionSpec getConnectionSpec() {
        return loadSpec.getConnectionSpec();
    }
}
