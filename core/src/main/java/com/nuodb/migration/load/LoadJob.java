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
import com.nuodb.migration.MigrationException;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.ConnectionServices;
import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.dialect.DialectResolver;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.Table;
import com.nuodb.migration.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migration.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migration.jdbc.model.ValueModelList;
import com.nuodb.migration.jdbc.query.*;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessProvider;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import com.nuodb.migration.resultset.catalog.Catalog;
import com.nuodb.migration.resultset.catalog.CatalogEntry;
import com.nuodb.migration.resultset.catalog.CatalogReader;
import com.nuodb.migration.resultset.format.FormatFactory;
import com.nuodb.migration.resultset.format.FormatInput;
import com.nuodb.migration.resultset.format.value.ValueFormatModel;
import com.nuodb.migration.resultset.format.value.ValueFormatRegistryResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.io.Closeables.closeQuietly;
import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.utils.ValidationUtils.isNotNull;

/**
 * @author Sergey Bushik
 */
public class LoadJob extends JobBase {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private ConnectionProvider connectionProvider;
    private TimeZone timeZone;
    private Map<String, Object> attributes;
    private Catalog catalog;
    private DialectResolver dialectResolver;
    private FormatFactory formatFactory;
    private ValueFormatRegistryResolver valueFormatRegistryResolver;
    private InsertType insertType;
    private Map<String, InsertType> tableInsertTypes;

    @Override
    public void execute(JobExecution execution) throws Exception {
        validate();
        execute(new LoadJobExecution(execution));
    }

    protected void validate() {
        isNotNull(getCatalog(), "Catalog is required");
        isNotNull(getConnectionProvider(), "Connection provider is required");
        isNotNull(getDialectResolver(), "Dialect resolver is required");
        isNotNull(getValueFormatRegistryResolver(), "Value format registry resolver is required");
    }

    protected void execute(LoadJobExecution execution) throws SQLException {
        ConnectionServices connectionServices = getConnectionProvider().getConnectionServices();
        try {
            execution.setConnectionServices(connectionServices);
            load(execution);
        } finally {
            close(connectionServices);
        }
    }

    protected void load(LoadJobExecution execution) throws SQLException {
        ConnectionServices connectionServices = execution.getConnectionServices();

        Connection connection = connectionServices.getConnection();
        InspectionManager inspectionManager = new InspectionManager();
        inspectionManager.setDialectResolver(getDialectResolver());
        inspectionManager.setConnection(connection);
        Database database = inspectionManager.inspect(
                new TableInspectionScope(connectionServices.getCatalog(), connectionServices.getSchema()),
                MetaDataType.DATABASE, MetaDataType.CATALOG, MetaDataType.SCHEMA, MetaDataType.TABLE, MetaDataType.COLUMN
        ).getObject(MetaDataType.DATABASE);

        execution.setDatabase(database);
        execution.setValueFormatRegistry(getValueFormatRegistryResolver().resolve(connection));

        CatalogReader catalogReader = getCatalog().getCatalogReader();
        execution.setCatalogReader(catalogReader);

        Dialect dialect = database.getDialect();
        try {
            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, timeZone);
            }
            for (CatalogEntry catalogEntry : catalogReader.readAll()) {
                load(execution, catalogEntry);
            }
            connection.commit();
        } catch (MigrationException exception) {
            connection.rollback();
            throw exception;
        } catch (Exception exception) {
            connection.rollback();
            throw new MigrationException(exception);
        } finally {
            if (dialect.supportsSessionTimeZone()) {
                dialect.setSessionTimeZone(connection, null);
            }
            closeQuietly(catalogReader);
        }
    }

    protected void load(LoadJobExecution execution, CatalogEntry catalogEntry) throws SQLException {
        CatalogReader catalogReader = execution.getCatalogReader();
        InputStream entryInput = catalogReader.getInputStream(catalogEntry);
        try {
            Dialect dialect = execution.getDatabase().getDialect();
            FormatInput formatInput = getFormatFactory().createInput(catalogEntry.getType());
            formatInput.setAttributes(getAttributes());
            formatInput.setValueFormatRegistry(execution.getValueFormatRegistry());
            formatInput.setValueAccessProvider(
                    new JdbcTypeValueAccessProvider(dialect.getJdbcTypeRegistry()));
            if (!dialect.supportsSessionTimeZone() && dialect.supportsStatementWithTimezone()) {
                formatInput.setTimeZone(getTimeZone());
            }
            formatInput.setInputStream(entryInput);
            formatInput.initInput();

            load(execution, formatInput, catalogEntry.getName());
        } finally {
            closeQuietly(entryInput);
        }
    }

    protected void load(final LoadJobExecution execution, final FormatInput formatInput,
                        String tableName) throws SQLException {
        formatInput.readBegin();
        ValueModelList<ValueFormatModel> valueFormatModelList = formatInput.getValueFormatModelList();
        if (valueFormatModelList.isEmpty()) {
            return;
        }
        Database database = execution.getDatabase();
        Table table = database.findTable(tableName);
        int index = 0;
        for (ValueFormatModel valueFormatModel : valueFormatModelList) {
            valueFormatModelList.get(index++).fromValueModel(table.getColumn(valueFormatModel.getName()));
        }
        final InsertQuery query = createInsertQuery(table, valueFormatModelList);

        StatementTemplate template = new StatementTemplate(execution.getConnectionServices().getConnection());
        template.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        return createStatement(connection, query);
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement preparedStatement) throws SQLException {
                        load(execution, formatInput, preparedStatement);
                    }
                }
        );
    }

    protected PreparedStatement createStatement(Connection connection, Query query) throws SQLException {
        return connection.prepareStatement(query.toQuery());
    }

    protected void load(LoadJobExecution execution, FormatInput formatInput,
                        PreparedStatement preparedStatement) throws SQLException {
        formatInput.setPreparedStatement(preparedStatement);
        formatInput.initInputModel();
        while (execution.isRunning() && formatInput.hasNextRow()) {
            formatInput.readRow();
            preparedStatement.executeUpdate();
        }
        formatInput.readEnd();
    }

    protected InsertQuery createInsertQuery(Table table, ValueModelList valueModelList) {
        InsertQueryBuilder builder = new InsertQueryBuilder();
        builder.setInsertType(getInsertType(table));
        builder.setQualifyNames(true);
        builder.setTable(table);
        if (valueModelList != null) {
            List<String> columns = Lists.newArrayList();
            for (int index = 0, length = valueModelList.size(); index < length; index++) {
                columns.add(valueModelList.get(index).getName());
            }
            builder.setColumns(columns);
        }
        return builder.build();
    }

    private InsertType getInsertType(Table table) {
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

    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    public void setConnectionProvider(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public InsertType getInsertType() {
        return insertType;
    }

    public void setInsertType(InsertType insertType) {
        this.insertType = insertType;
    }

    public void setTableInsertTypes(Map<String, InsertType> tableInsertTypes) {
        this.tableInsertTypes = tableInsertTypes;
    }

    public Map<String, InsertType> getTableInsertTypes() {
        return tableInsertTypes;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
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
