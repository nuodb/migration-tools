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
import com.google.common.collect.Lists;
import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.*;
import com.nuodb.migrator.backup.format.InputFormat;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.backup.format.value.ValueHandleList;
import com.nuodb.migrator.jdbc.commit.CommitStrategy;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.query.*;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.job.HasServicesJobBase;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.LoadJobSpec;
import com.nuodb.migrator.spec.ResourceSpec;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.format.value.ValueHandleListBuilder.newBuilder;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newSessionTimeZoneSetter;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class LoadJob extends HasServicesJobBase<LoadJobSpec> {

    private RowSetMapper rowSetMapper = new SimpleRowSetMapper();

    private Session targetSession;
    private BackupManager backupManager;
    private Map<String, Object> formatAttributes;
    private ValueFormatRegistry valueFormatRegistry;

    public LoadJob() {
    }

    public LoadJob(LoadJobSpec jobSpec) {
        super(jobSpec);
    }

    @Override
    protected void init() throws Exception {
        super.init();

        ResourceSpec inputSpec = getInputSpec();
        setFormatAttributes(inputSpec.getAttributes());
        setBackupManager(createBackupManager(inputSpec));
        
        Session targetSession = createSessionFactory().openSession();
        setTargetSession(targetSession);
        setFormatFactory(createFormatFactory());
        setValueFormatRegistry(createValueFormatRegistryResolver().resolve(targetSession.getConnection()));
    }

    protected BackupManager createBackupManager(ResourceSpec inputSpec) {
        return new XmlBackupManager(inputSpec.getPath());
    }

    protected SessionFactory createSessionFactory() {
        SessionFactory sessionFactory = newSessionFactory(createConnectionProviderFactory().
                createConnectionProvider(getTargetSpec()), createDialectResolver());
        sessionFactory.addSessionObserver(newSessionTimeZoneSetter(getTimeZone()));
        return sessionFactory;
    }

    @Override
    public void execute() throws Exception {
        Database database = inspect();
        Connection connection = getTargetSession().getConnection();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Commit using %s", getJobSpec().getCommitStrategy()));
            }
            Backup backup = getBackupManager().readBackup();
            for (RowSet rowSet : backup.getRowSets()) {
                load(rowSet, database);
            }
            connection.commit();
        } catch (MigratorException exception) {
            connection.rollback();
            throw exception;
        } catch (Exception exception) {
            connection.rollback();
            throw new LoadException(exception);
        }
    }

    @Override
    public void release() throws Exception {
        close(getTargetSession());
    }

    protected Database inspect() throws SQLException {
        InspectionScope inspectionScope = new TableInspectionScope(
                getTargetSpec().getCatalog(), getTargetSpec().getSchema());
        return createInspectionManager().inspect(getTargetSession().getConnection(), inspectionScope,
                DATABASE, CATALOG, SCHEMA, TABLE, COLUMN).getObject(DATABASE);
    }

    protected void load(final RowSet rowSet, Database database) throws SQLException {
        if (!isEmpty(rowSet.getChunks())) {
            final Connection connection = getTargetSession().getConnection();
            final Table table = getRowSetMapper().map(rowSet, database);
            if (table != null) {
                final InsertQuery query = createInsertQuery(table, rowSet.getColumns());
                final StatementTemplate template = new StatementTemplate(connection);
                template.executeStatement(
                        new StatementFactory<PreparedStatement>() {
                            @Override
                            public PreparedStatement createStatement(Connection connection) throws SQLException {
                                return connection.prepareStatement(query.toString());
                            }
                        },
                        new StatementCallback<PreparedStatement>() {
                            @Override
                            public void executeStatement(PreparedStatement statement)
                                    throws SQLException {
                                load(rowSet, table, statement, query);
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

    protected void load(RowSet rowSet, Table table, PreparedStatement statement, Query query) throws SQLException {
        InputFormat inputFormat = getFormatFactory().createInputFormat(
                rowSet.getBackup().getFormat(), getFormatAttributes());
        ValueHandleList valueHandleList = createValueHandleList(rowSet, table, statement);
        CommitStrategy commitStrategy = getJobSpec().getCommitStrategy();
        for (Chunk chunk : rowSet.getChunks()) {
            inputFormat.setRowSet(rowSet);
            inputFormat.setValueHandleList(valueHandleList);
            inputFormat.setInputStream(getBackupManager().openInput(chunk.getName()));
            inputFormat.init();
            if (logger.isTraceEnabled()) {
                logger.trace(format("Loading %d rows from %s chunk to %s table",
                        chunk.getRowCount(), chunk.getName(), table.getQualifiedName(null)));
            }
            inputFormat.readStart();
            long row = 0;
            while (inputFormat.read()) {
                try {
                    statement.execute();
                    if (commitStrategy != null) {
                        commitStrategy.onExecute(statement, query);
                    }
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

    protected ValueHandleList createValueHandleList(final RowSet rowSet, final Table table,
                                                    PreparedStatement statement) throws SQLException {
        Iterable<com.nuodb.migrator.jdbc.metadata.Column> columns = transform(rowSet.getColumns(),
                new Function<Column, com.nuodb.migrator.jdbc.metadata.Column>() {
                    @Override
                    public com.nuodb.migrator.jdbc.metadata.Column apply(Column column) {
                        return table.getColumn(column.getName());
                    }
                });
        return newBuilder(statement).
                withColumns(newArrayList(columns)).
                withDialect(getTargetSession().getDialect()).
                withTimeZone(getTimeZone()).
                withValueFormatRegistry(getValueFormatRegistry()).build();
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

    public Session getTargetSession() {
        return targetSession;
    }

    public void setTargetSession(Session targetSession) {
        this.targetSession = targetSession;
    }

    public BackupManager getBackupManager() {
        return backupManager;
    }

    public void setBackupManager(BackupManager backupManager) {
        this.backupManager = backupManager;
    }

    public Map<String, Object> getFormatAttributes() {
        return formatAttributes;
    }

    public void setFormatAttributes(Map<String, Object> formatAttributes) {
        this.formatAttributes = formatAttributes;
    }

    public ValueFormatRegistry getValueFormatRegistry() {
        return valueFormatRegistry;
    }

    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        this.valueFormatRegistry = valueFormatRegistry;
    }

    public RowSetMapper getRowSetMapper() {
        return rowSetMapper;
    }

    public void setRowSetMapper(RowSetMapper rowSetMapper) {
        this.rowSetMapper = rowSetMapper;
    }

    protected Map<String, InsertType> getTableInsertTypes() {
        return getJobSpec().getTableInsertTypes();
    }

    protected ResourceSpec getInputSpec() {
        return getJobSpec().getInputSpec();
    }

    protected InsertType getInsertType() {
        return getJobSpec().getInsertType();
    }

    protected TimeZone getTimeZone() {
        return getJobSpec().getTimeZone();
    }

    protected ConnectionSpec getTargetSpec() {
        return getJobSpec().getTargetSpec();
    }
}
