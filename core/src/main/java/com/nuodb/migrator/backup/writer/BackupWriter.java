/**
 * Copyright (c) 2014, NuoDB, Inc.
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
package com.nuodb.migrator.backup.writer;

import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.Backup;
import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.backup.QueryRowSet;
import com.nuodb.migrator.backup.TableRowSet;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.csv.CsvAttributes;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.jdbc.JdbcUtils;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.HasTables;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.session.Work;
import com.nuodb.migrator.jdbc.split.QuerySplitter;
import com.nuodb.migrator.spec.TableSpec;
import com.nuodb.migrator.utils.BlockingThreadPoolExecutor;
import org.slf4j.Logger;

import java.sql.Connection;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.split.QuerySplitters.*;
import static com.nuodb.migrator.jdbc.split.RowCountStrategies.newCachingStrategy;
import static com.nuodb.migrator.jdbc.split.RowCountStrategies.newHandlerStrategy;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.ArrayUtils.indexOf;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored", "SynchronizationOnLocalVariableOrMethodParameter"})
public class BackupWriter {

    public static final boolean WRITE_DATA = true;
    public static final boolean WRITE_SCHEMA = true;
    public static final String FORMAT = CsvAttributes.FORMAT;
    public static final int THREADS = getRuntime().availableProcessors();

    protected final transient Logger logger = getLogger(getClass());
    private BackupOps backupOps;
    private Integer threads = THREADS;
    private String format = FORMAT;
    private Executor executor;
    private TimeZone timeZone;
    private Session sourceSession;
    private SessionFactory sourceSessionFactory;
    private Map<String, Object> formatAttributes = newHashMap();
    private FormatFactory formatFactory;
    private ValueFormatRegistry valueFormatRegistry;
    private Database database;
    private QueryLimit queryLimit;
    private Collection<ExportQuery> exportQueries = newLinkedHashSet();
    private boolean writeData = WRITE_DATA;
    private boolean writeSchema = WRITE_SCHEMA;

    public BackupWriter() {
    }

    public void addQuery(String query) {
        addExportQuery(createExportQuery(query));
    }

    public void addTable(Table table) {
        addTable(table, table.getColumns());
    }

    public void addTable(Table table, Collection<Column> columns) {
        addTable(table, columns, null);
    }

    public void addTable(Table table, Collection<Column> columns, String filter) {
        addTable(table, columns, filter, getQueryLimit());
    }

    /**
     * Adds a table with specified columns and filtering clause to the writing queue.
     *
     * @param table      to include into dump.
     * @param columns    only specified columns will be writing.
     * @param filter     to filter rows with.
     * @param queryLimit which will be used for creating "pages", if query limit is null no limiting query splitter will
     *                   be constructed.
     */
    public void addTable(Table table, Collection<Column> columns,
                         String filter, QueryLimit queryLimit) {
        addExportQuery(createExportQuery(table, columns, filter, queryLimit));
    }

    /**
     * Adds all tables contained within schema, catalog or database.
     *
     * @param tables to include into dump.
     */
    public void addTables(HasTables tables) {
        addTables(tables, null);
    }

    /**
     * Adds tables restricted to certain types contained within schema, catalog or database.
     *
     * @param tables to include into dump.
     * @param tableTypes allowed table types.
     */
    public void addTables(HasTables tables, String[] tableTypes) {
        for (Table table : tables.getTables()) {
            if (isEmpty(tableTypes) || indexOf(tableTypes, table.getType()) != -1) {
                addTable(table);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(format("Table %s %s is not in the allowed types, table skipped",
                            table.getQualifiedName(null), table.getType()));
                }
            }
        }
    }

    public void addTables(Collection<TableSpec> tableSpecs) {
        for (TableSpec tableSpec : tableSpecs) {
            Table table = database.findTable(tableSpec.getTable());
            Collection<Column> columns;
            if (isEmpty(tableSpec.getColumns())) {
                columns = table.getColumns();
            } else {
                columns = newArrayList();
                for (String column : tableSpec.getColumns()) {
                    columns.add(table.getColumn(column));
                }
            }
            String filter = tableSpec.getFilter();
            addTable(table, columns, filter);
        }
    }

    protected void addExportQuery(ExportQuery exportQuery) {
        exportQueries.add(exportQuery);
    }

    public void write(Map context) throws Exception {
        write(createBackup(), context);
    }

    protected Backup createBackup() {
        Backup backup = new Backup();
        backup.setFormat(getFormat());
        return backup;
    }

    public void write(Backup backup, Map context) throws Exception {
        write(backup, createBackupWriterContext(context));
    }

    protected BackupWriterContext createBackupWriterContext(Map context) {
        BackupWriterContext backupWriterContext =
                new SimpleBackupWriterContext();
        backupWriterContext.setBackupOps(getBackupOps());
        backupWriterContext.setBackupOpsContext(context);
        Executor executor = getExecutor();
        if (executor == null) {
            executor = createExecutor();
        }
        backupWriterContext.setExecutor(executor);
        backupWriterContext.setExportQueryManager(createExportQueryManager());
        backupWriterContext.setFormat(getFormat());
        backupWriterContext.setDatabase(getDatabase());
        backupWriterContext.setFormatAttributes(getFormatAttributes());
        backupWriterContext.setFormatFactory(getFormatFactory());
        backupWriterContext.setSourceSession(getSourceSession());
        backupWriterContext.setSourceSessionFactory(getSourceSessionFactory());
        backupWriterContext.setThreads(getThreads());
        backupWriterContext.setTimeZone(getTimeZone());
        backupWriterContext.setValueFormatRegistry(getValueFormatRegistry());
        return backupWriterContext;
    }

    protected Executor createExecutor() {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Using blocking thread pool with %d thread(s)", getThreads()));
        }
        return new BlockingThreadPoolExecutor(getThreads(), 100L, MILLISECONDS);
    }

    protected ExportQueryManager createExportQueryManager() {
        return new SimpleExportQueryManager();
    }

    public void write(Backup backup, BackupWriterContext backupWriterContext) throws Exception {
        boolean awaitTermination = true;
        try {
            if (isWriteData()) {
                Connection connection = getSourceSession().getConnection();
                for (ExportQuery exportQuery : getExportQueries()) {
                    backup.addRowSet(exportQuery.getRowSet());
                    while (exportQuery.getQuerySplitter().hasNextQuerySplit(connection)) {
                        Work work = createWork(backupWriterContext, exportQuery);
                        executeWork(backupWriterContext, work);
                    }
                }
            }
            backup.setDatabase(isWriteSchema() ? getDatabase() : null);
            backupWriterContext.getBackupOps().write(backup,
                    backupWriterContext.getBackupOpsContext());
        } catch (Throwable failure) {
            awaitTermination = false;
            throw failure instanceof MigratorException ?
                    (MigratorException) failure : new BackupWriterException(failure);
        } finally {
            backupWriterContext.close(awaitTermination);
        }
    }

    protected Work createWork(BackupWriterContext backupWriterContext, ExportQuery exportQuery) throws Exception {
        Connection connection = getSourceSession().getConnection();
        QuerySplitter querySplitter = exportQuery.getQuerySplitter();
        return new ExportQueryWork(backupWriterContext, exportQuery,
                querySplitter.getNextQuerySplit(connection), querySplitter.hasNextQuerySplit(connection));
    }

    protected void executeWork(final BackupWriterContext backupWriterContext, final Work work) {
        final ExportQueryManager exportQueryManager = backupWriterContext.getExportQueryManager();
        final Executor executor = backupWriterContext.getExecutor();
        final SessionFactory sessionFactory = backupWriterContext.getSourceSessionFactory();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Session session = null;
                try {
                    session = sessionFactory.openSession();
                    session.execute(work, exportQueryManager);
                } catch (Exception exception) {
                    exportQueryManager.failure(work, exception);
                } finally {
                    JdbcUtils.close(session);
                }
            }
        });
    }

    protected ExportQuery createExportQuery(String query) {
        return new ExportQuery(createQuerySplitter(query), new QueryRowSet(query));
    }

    protected QuerySplitter createQuerySplitter(String query) {
        return newNoLimitSplitter(newQuery(query));
    }

    protected ExportQuery createExportQuery(Table table, Collection<Column> columns, String filter,
                                            QueryLimit queryLimit) {
        return new ExportTable(table, columns, filter,
                createQuerySplitter(table, columns, filter, queryLimit), new TableRowSet(table));
    }

    protected QuerySplitter createQuerySplitter(Table table, Collection<Column> columns, String filter,
                                                QueryLimit queryLimit) {
        QuerySplitter querySplitter;
        Query query = newQuery(table, columns, filter);
        Dialect dialect = getSourceSession().getDialect();
        if (queryLimit != null && supportsLimitSplitter(dialect, table, filter)) {
            querySplitter = newLimitSplitter(dialect, newCachingStrategy(newHandlerStrategy(
                    dialect.createRowCountHandler(table, null, filter, EXACT))), query, queryLimit);
        } else {
            querySplitter = newNoLimitSplitter(query);
        }
        return querySplitter;
    }

    public boolean isWriteData() {
        return writeData;
    }

    public void setWriteData(boolean writeData) {
        this.writeData = writeData;
    }

    public boolean isWriteSchema() {
        return writeSchema;
    }

    public void setWriteSchema(boolean writeSchema) {
        this.writeSchema = writeSchema;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
        this.executor = null;
    }

    public BackupOps getBackupOps() {
        return backupOps;
    }

    public void setBackupOps(BackupOps backupOps) {
        this.backupOps = backupOps;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format != null ? format: FORMAT;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public Session getSourceSession() {
        return sourceSession;
    }

    public void setSourceSession(Session sourceSession) {
        this.sourceSession = sourceSession;
    }

    public SessionFactory getSourceSessionFactory() {
        return sourceSessionFactory;
    }

    public void setSourceSessionFactory(SessionFactory sourceSessionFactory) {
        this.sourceSessionFactory = sourceSessionFactory;
    }

    public Map<String, Object> getFormatAttributes() {
        return formatAttributes;
    }

    public void setFormatAttributes(Map<String, Object> formatAttributes) {
        this.formatAttributes = formatAttributes;
    }

    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    public ValueFormatRegistry getValueFormatRegistry() {
        return valueFormatRegistry;
    }

    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        this.valueFormatRegistry = valueFormatRegistry;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(QueryLimit queryLimit) {
        this.queryLimit = queryLimit;
    }

    public Collection<ExportQuery> getExportQueries() {
        return exportQueries;
    }

    public void setExportQueries(Collection<ExportQuery> exportQueries) {
        this.exportQueries = exportQueries;
    }
}
