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
package com.nuodb.migration.load;

import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.ConnectionServices;
import com.nuodb.migration.jdbc.dialect.DatabaseDialect;
import com.nuodb.migration.jdbc.dialect.DatabaseDialectResolver;
import com.nuodb.migration.jdbc.model.*;
import com.nuodb.migration.jdbc.query.*;
import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessProvider;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import com.nuodb.migration.resultset.catalog.Catalog;
import com.nuodb.migration.resultset.catalog.CatalogEntry;
import com.nuodb.migration.resultset.catalog.CatalogReader;
import com.nuodb.migration.resultset.format.ResultSetFormatFactory;
import com.nuodb.migration.resultset.format.ResultSetInput;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormatRegistry;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormatRegistryResolver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.io.Closeables.closeQuietly;
import static com.nuodb.migration.jdbc.model.ObjectType.*;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

/**
 * @author Sergey Bushik
 */
public class LoadJob extends JobBase {

    protected final Log log = LogFactory.getLog(getClass());
    private ConnectionProvider connectionProvider;
    private TimeZone timeZone;
    private Map<String, String> attributes;
    private Catalog catalog;
    private DatabaseDialectResolver databaseDialectResolver;
    private ResultSetFormatFactory resultSetFormatFactory;
    private JdbcTypeValueFormatRegistryResolver jdbcTypeValueFormatRegistryResolver;

    @Override
    public void execute(final JobExecution execution) throws Exception {
        ConnectionServices connectionServices = null;
        try {
            connectionServices = connectionProvider.getConnectionServices();
            load(execution, connectionServices);
        } finally {
            if (connectionServices != null) {
                connectionServices.close();
            }
        }
    }

    protected void load(JobExecution execution, ConnectionServices connectionServices) throws SQLException {
        DatabaseInspector databaseInspector = connectionServices.getDatabaseInspector();
        databaseInspector.withObjectTypes(CATALOG, SCHEMA, TABLE, COLUMN);
        databaseInspector.withDatabaseDialectResolver(databaseDialectResolver);
        Database database = databaseInspector.inspect();

        DatabaseDialect databaseDialect = database.getDatabaseDialect();
        databaseDialect.setSupportedTransactionIsolation(connectionServices.getConnection(),
                new int[]{TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED});

        DatabaseMetaData metaData = connectionServices.getConnection().getMetaData();
        JdbcTypeValueFormatRegistry jdbcTypeValueFormatRegistry =
                jdbcTypeValueFormatRegistryResolver.resolve(metaData);

        CatalogReader catalogReader = getCatalog().getCatalogReader();
        try {
            for (CatalogEntry catalogEntry : catalogReader.getEntries()) {
                load(execution, connectionServices, jdbcTypeValueFormatRegistry, database, catalogReader, catalogEntry);
            }
            connectionServices.getConnection().commit();
        } finally {
            closeQuietly(catalogReader);
        }
    }

    protected void load(JobExecution execution, ConnectionServices connectionServices,
                        JdbcTypeValueFormatRegistry jdbcTypeValueFormatRegistry, Database database,
                        CatalogReader catalogReader, CatalogEntry catalogEntry) throws SQLException {
        InputStream entryInput = catalogReader.getEntryInput(catalogEntry);
        try {
            final ResultSetInput resultSetInput = getResultSetFormatFactory().createInput(catalogEntry.getType());
            resultSetInput.setAttributes(getAttributes());
            resultSetInput.setTimeZone(getTimeZone());
            resultSetInput.setInputStream(entryInput);
            resultSetInput.setJdbcTypeValueFormatRegistry(jdbcTypeValueFormatRegistry);

            JdbcTypeRegistry jdbcTypeRegistry = database.getDatabaseDialect().getJdbcTypeRegistry();
            JdbcTypeValueAccessProvider jdbcTypeValueAccessProvider = new JdbcTypeValueAccessProvider(jdbcTypeRegistry);
            resultSetInput.setJdbcTypeValueAccessProvider(jdbcTypeValueAccessProvider);

            load(execution, connectionServices, database, resultSetInput, catalogEntry.getName());
        } finally {
            closeQuietly(entryInput);
        }
    }

    protected void load(final JobExecution execution, ConnectionServices connectionServices, Database database,
                        final ResultSetInput resultSetInput, String tableName) throws SQLException {
        resultSetInput.readBegin();

        ColumnModelSet<ColumnModel> columns = resultSetInput.getColumnModelSet();
        Table table = database.findTable(tableName);
        for (ColumnModel column : columns) {
            column.copy(table.getColumn(column.getName()));
        }
        final InsertQuery query = createInsertQuery(table, columns);

        QueryTemplate queryTemplate = new QueryTemplate(connectionServices.getConnection());
        queryTemplate.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return createStatement(connection, query);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement preparedStatement) throws SQLException {
                        load(preparedStatement, resultSetInput, execution);
                    }
                }
        );
    }

    protected PreparedStatement createStatement(Connection connection, Query query) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Prepare SQL: %s", query.toQuery()));
        }
        return connection.prepareStatement(query.toQuery());
    }

    protected void load(PreparedStatement preparedStatement, ResultSetInput resultSetInput,
                        JobExecution execution) throws SQLException {
        resultSetInput.setPreparedStatement(preparedStatement);
        while (resultSetInput.hasNextRow() && execution.isRunning()) {
            resultSetInput.readRow();
            preparedStatement.executeUpdate();
        }
        resultSetInput.readEnd();
    }

    protected InsertQuery createInsertQuery(Table table, ColumnModelSet columnModelSet) {
        InsertQueryBuilder builder = new InsertQueryBuilder();
        builder.setQualifyNames(true);
        builder.setTable(table);
        if (columnModelSet != null) {
            List<String> columns = Lists.newArrayList();
            for (int index = 0, length = columnModelSet.size(); index < length; index++) {
                columns.add(columnModelSet.get(index).getName());
            }
            builder.setColumns(columns);
        }
        return builder.build();
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

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    public DatabaseDialectResolver getDatabaseDialectResolver() {
        return databaseDialectResolver;
    }

    public void setDatabaseDialectResolver(DatabaseDialectResolver databaseDialectResolver) {
        this.databaseDialectResolver = databaseDialectResolver;
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
