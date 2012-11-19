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
import com.nuodb.migration.jdbc.ConnectionServices;
import com.nuodb.migration.jdbc.ConnectionServicesCallback;
import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.model.*;
import com.nuodb.migration.jdbc.query.*;
import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessProvider;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import com.nuodb.migration.result.catalog.Catalog;
import com.nuodb.migration.result.catalog.CatalogEntry;
import com.nuodb.migration.result.catalog.CatalogReader;
import com.nuodb.migration.result.format.ResultFormatFactory;
import com.nuodb.migration.result.format.ResultInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.google.common.io.Closeables.closeQuietly;
import static com.nuodb.migration.jdbc.ConnectionServicesFactory.createConnectionServices;
import static com.nuodb.migration.jdbc.model.ObjectType.COLUMN;
import static com.nuodb.migration.jdbc.model.ObjectType.TABLE;

/**
 * @author Sergey Bushik
 */
public class LoadJob extends JobBase {

    protected final Log log = LogFactory.getLog(getClass());
    private ConnectionProvider connectionProvider;
    private String inputType;
    private Map<String, String> inputAttributes;
    private Catalog catalog;
    private ResultFormatFactory resultFormatFactory;

    @Override
    public void execute(final JobExecution execution) throws Exception {
        ConnectionServicesCallback.execute(createConnectionServices(connectionProvider),
                new ConnectionServicesCallback() {
                    @Override
                    public void execute(ConnectionServices services) throws SQLException {
                        DatabaseInspector databaseInspector = services.getDatabaseInspector();
                        databaseInspector.withObjectTypes(TABLE, COLUMN);
                        Database database = databaseInspector.inspect();

                        CatalogReader reader = catalog.getReader();
                        try {
                            for (CatalogEntry entry : reader.getEntries()) {
                                load(execution, services, database, reader, entry);
                            }
                            services.getConnection().commit();
                        } finally {
                            closeQuietly(reader);
                        }
                    }
                });
    }

    protected void load(final JobExecution execution, final ConnectionServices services, final Database database,
                        final CatalogReader reader, final CatalogEntry entry) throws SQLException {
        InputStream entryInput = reader.getEntryInput(entry);
        try {
            final ResultInput resultInput = resultFormatFactory.createInput(entry.getType());
            resultInput.setAttributes(inputAttributes);
            resultInput.setInputStream(entryInput);

            JdbcTypeRegistry jdbcTypeRegistry = database.getDatabaseDialect().getJdbcTypeRegistry();
            resultInput.setJdbcTypeValueAccessProvider(new JdbcTypeValueAccessProvider(jdbcTypeRegistry));
            resultInput.initInput();

            load(execution, services, database, resultInput, entry.getName());
        } finally {
            closeQuietly(entryInput);
        }
    }

    protected void load(final JobExecution execution, final ConnectionServices services, final Database database,
                        final ResultInput resultInput, final String tableName) throws SQLException {
        resultInput.readBegin();
        ColumnSetModel columnSetModel = resultInput.getColumnSetModel();
        Table table = database.findTable(tableName);
        mergeColumnSetModel(table, columnSetModel);
        final InsertQuery query = createInsertQuery(table, columnSetModel);

        QueryTemplate queryTemplate = new QueryTemplate(services.getConnection());
        queryTemplate.execute(
                new StatementCreator<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Preparing SQL query %s", query.toQuery()));
                        }
                        return connection.prepareStatement(query.toQuery());
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void execute(PreparedStatement preparedStatement) throws SQLException {
                        resultInput.setPreparedStatement(preparedStatement);

                        resultInput.initModel();
                        while (resultInput.canReadRow() && execution.isRunning()) {
                            resultInput.readRow();
                            preparedStatement.executeUpdate();
                        }
                        resultInput.readEnd();
                    }
                }
        );
    }

    protected void mergeColumnSetModel(Table table, ColumnSetModel columnSetModel) {
        for (int index = 0; index < columnSetModel.getLength(); index++) {
            String name = columnSetModel.getName(index);
            Column column = table.getColumn(name);
            columnSetModel.setTypeCode(index, column.getTypeCode());
            columnSetModel.setPrecision(index, column.getPrecision());
            columnSetModel.setScale(index, column.getScale());
        }
    }

    protected InsertQuery createInsertQuery(Table table, ColumnSetModel columnSetModel) {
        InsertQueryBuilder builder = new InsertQueryBuilder();
        builder.setQualifyNames(true);
        builder.setTable(table);
        if (columnSetModel != null) {
            int columnCount = columnSetModel.getLength();
            List<String> columns = Lists.newArrayList();
            for (int index = 0; index < columnCount; index++) {
                ColumnModel columnModel = columnSetModel.item(index);
                columns.add(columnModel.getName());
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

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    public String getInputType() {
        return inputType;
    }

    public void setInputType(String inputType) {
        this.inputType = inputType;
    }

    public Map<String, String> getInputAttributes() {
        return inputAttributes;
    }

    public void setInputAttributes(Map<String, String> inputAttributes) {
        this.inputAttributes = inputAttributes;
    }

    public ResultFormatFactory getResultFormatFactory() {
        return resultFormatFactory;
    }

    public void setResultFormatFactory(ResultFormatFactory resultFormatFactory) {
        this.resultFormatFactory = resultFormatFactory;
    }
}
