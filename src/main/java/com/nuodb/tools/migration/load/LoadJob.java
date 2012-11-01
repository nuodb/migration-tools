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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.nuodb.tools.migration.output.catalog.Entry;
import com.nuodb.tools.migration.output.catalog.EntryCatalog;
import com.nuodb.tools.migration.output.catalog.EntryReader;
import com.nuodb.tools.migration.output.format.ColumnDataModel;
import com.nuodb.tools.migration.output.format.DataFormatFactory;
import com.nuodb.tools.migration.output.format.InputDataFormat;
import com.nuodb.tools.migration.output.format.csv.CsvInputDataFormat;
import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.connection.ConnectionCallback;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.*;
import com.nuodb.tools.migration.jdbc.query.*;
import com.nuodb.tools.migration.job.JobBase;
import com.nuodb.tools.migration.job.JobExecution;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import static com.nuodb.tools.migration.jdbc.metamodel.ObjectType.*;

/**
 * @author Sergey Bushik
 */
public class LoadJob extends JobBase {

    protected final Log log = LogFactory.getLog(getClass());
    private JdbcServices jdbcServices;
    private EntryCatalog entryCatalog;
    private String inputType;
    private Map<String, String> inputAttributes;
    private DataFormatFactory<InputDataFormat> inputDataFormatFactory;

    @Override
    public void execute(final JobExecution execution) throws Exception {
        ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        connectionProvider.execute(new ConnectionCallback() {
            @Override
            public void execute(Connection connection) throws SQLException {
                DatabaseInspector databaseInspector = jdbcServices.getDatabaseIntrospector();
                databaseInspector.withObjectTypes(CATALOG, SCHEMA, TABLE, COLUMN);
                databaseInspector.withConnection(connection);
                Database database = databaseInspector.inspect();

                EntryReader entryReader = entryCatalog.openReader();
                try {
                    for (Entry entry : entryReader.getEntries()) {
                        load(execution, connection, database, entryReader, entry);
                    }
                    connection.commit();
                } finally {
                    entryReader.close();
                }
            }
        });
    }

    protected void load(JobExecution execution, Connection connection, Database database, EntryReader reader,
                        Entry entry) throws SQLException {
        InputStream entryInput = reader.getEntryInput(entry);
        try {
            final CsvInputDataFormat format = (CsvInputDataFormat) inputDataFormatFactory.createDataFormat(entry.getType());
            format.setAttributes(inputAttributes);
            format.setInputStream(entryInput);
            format.setJdbcTypeAccess(jdbcServices.getJdbcTypeAccessor());
            format.init();
            final Table table = database.findTable(entry.getName());
            final ColumnDataModel model = format.read();
            final InsertQuery query = createInsertQuery(table, model);
            final ColumnSetModel columnSetModel = new ColumnSetModelImpl(model.getColumns(),
                    Collections2.transform(model.getColumns(), new Function<String, Integer>() {
                        @Override
                        public Integer apply(String column) {
                            ColumnType type = table.getColumn(column).getType();
                            return type.getTypeCode();
                        }
                    }));
            QueryTemplate queryTemplate = new QueryTemplate(connection);
            queryTemplate.execute(
                    new StatementBuilder<PreparedStatement>() {
                        @Override
                        public PreparedStatement build(Connection connection) throws SQLException {
                            return connection.prepareStatement(query.toQuery());
                        }
                    },
                    new StatementCallback<PreparedStatement>() {
                        @Override
                        public void execute(PreparedStatement statement) throws SQLException {
                            while (format.bind(statement, columnSetModel)) {
                                statement.executeUpdate();
                            }
                        }
                    }
            );
        } finally {
            IOUtils.closeQuietly(entryInput);
        }
    }

    protected InsertQuery createInsertQuery(Table table, ColumnDataModel model) {
        InsertQueryBuilder builder = new InsertQueryBuilder();
        builder.setQualifyNames(true);
        builder.setTable(table);
        builder.setColumns(model.getColumns());
        return builder.build();
    }

    public JdbcServices getJdbcServices() {
        return jdbcServices;
    }

    public void setJdbcServices(JdbcServices jdbcServices) {
        this.jdbcServices = jdbcServices;
    }

    public EntryCatalog getEntryCatalog() {
        return entryCatalog;
    }

    public void setEntryCatalog(EntryCatalog entryCatalog) {
        this.entryCatalog = entryCatalog;
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

    public DataFormatFactory<InputDataFormat> getInputDataFormatFactory() {
        return inputDataFormatFactory;
    }

    public void setInputDataFormatFactory(DataFormatFactory<InputDataFormat> inputDataFormatFactory) {
        this.inputDataFormatFactory = inputDataFormatFactory;
    }
}
