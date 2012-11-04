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
package com.nuodb.tools.migration.load;

import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.connection.ConnectionCallback;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.ColumnSetModel;
import com.nuodb.tools.migration.jdbc.metamodel.Database;
import com.nuodb.tools.migration.jdbc.metamodel.DatabaseInspector;
import com.nuodb.tools.migration.jdbc.metamodel.Table;
import com.nuodb.tools.migration.jdbc.query.*;
import com.nuodb.tools.migration.job.JobBase;
import com.nuodb.tools.migration.job.JobExecution;
import com.nuodb.tools.migration.result.catalog.ResultCatalog;
import com.nuodb.tools.migration.result.catalog.ResultEntry;
import com.nuodb.tools.migration.result.catalog.ResultEntryReader;
import com.nuodb.tools.migration.result.format.ResultFormatFactory;
import com.nuodb.tools.migration.result.format.ResultInput;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import static com.nuodb.tools.migration.jdbc.metamodel.ObjectType.COLUMN;
import static com.nuodb.tools.migration.jdbc.metamodel.ObjectType.TABLE;

/**
 * @author Sergey Bushik
 */
public class LoadJob extends JobBase {

    protected final Log log = LogFactory.getLog(getClass());
    private JdbcServices jdbcServices;
    private String inputType;
    private Map<String, String> inputAttributes;
    private ResultCatalog resultCatalog;
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

                ResultEntryReader reader = resultCatalog.openReader();
                try {
                    for (ResultEntry entry : reader.getEntries()) {
                        load(execution, connection, database, reader, entry);
                    }
                    connection.commit();
                } finally {
                    reader.close();
                }
            }
        });
    }

    protected void load(final JobExecution execution, Connection connection, Database database,
                        ResultEntryReader reader, ResultEntry entry) throws SQLException {
        InputStream entryInput = reader.getEntryInput(entry);
        try {
            final ResultInput resultInput = resultFormatFactory.createInput(entry.getType());
            resultInput.setAttributes(inputAttributes);
            resultInput.setInputStream(entryInput);
            resultInput.setJdbcTypeAccessor(jdbcServices.getJdbcTypeAccessor());
            resultInput.initInput();

            load(execution, connection, database, resultInput, entry.getName());
        } finally {
            IOUtils.closeQuietly(entryInput);
        }
    }

    protected void load(final JobExecution execution, Connection connection, Database database,
                        final ResultInput resultInput, String tableName) throws SQLException {
        resultInput.readBegin();
        ColumnSetModel columnSetModel = resultInput.getColumnSetModel();
        Table table = database.findTable(tableName);
        mergeColumnSetModel(table, columnSetModel);
        final InsertQuery query = createInsertQuery(table, columnSetModel);

        QueryTemplate queryTemplate = new QueryTemplate(connection);
        queryTemplate.execute(
                new StatementBuilder<PreparedStatement>() {
                    @Override
                    public PreparedStatement build(Connection connection) throws SQLException {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Prepare SQL query %s", query.toQuery()));
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
        for (int index = 0; index < columnSetModel.getColumnCount(); index++) {
            String column = columnSetModel.getColumn(index);
            int columnType = table.getColumn(column).getType().getTypeCode();
            columnSetModel.setColumnType(index, columnType);
        }
    }

    protected InsertQuery createInsertQuery(Table table, ColumnSetModel columnSetModel) {
        InsertQueryBuilder builder = new InsertQueryBuilder();
        builder.setQualifyNames(true);
        builder.setTable(table);
        if (columnSetModel != null) {
            builder.setColumns(Arrays.asList(columnSetModel.getColumns()));
        }
        return builder.build();
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
