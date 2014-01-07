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

import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.Backup;
import com.nuodb.migrator.backup.BackupManager;
import com.nuodb.migrator.backup.QueryRowSet;
import com.nuodb.migrator.backup.TableRowSet;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.session.Work;
import com.nuodb.migrator.jdbc.session.WorkManager;
import com.nuodb.migrator.jdbc.split.QuerySplitter;
import org.slf4j.Logger;

import java.sql.Connection;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.split.QuerySplitters.*;
import static com.nuodb.migrator.jdbc.split.RowCountStrategies.newCachingStrategy;
import static com.nuodb.migrator.jdbc.split.RowCountStrategies.newHandlerStrategy;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public class DumpWriter implements DumpQueryContext {

    protected final transient Logger logger = getLogger(getClass());
    private QueryLimit queryLimit;
    private Collection<DumpQuery> dumpQueries = newLinkedHashSet();
    private DumpQueryContext dumpQueryContext = new SimpleDumpQueryContext();

    public void addQuery(String query) {
        addDumpQuery(createDumpQuery(query));
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
    public void addTable(Table table, Collection<Column> columns, String filter, QueryLimit queryLimit) {
        addDumpQuery(createDumpQuery(table, columns, filter, queryLimit));
    }

    protected void addDumpQuery(DumpQuery dumpQuery) {
        dumpQueries.add(dumpQuery);
    }

    public Backup write() throws Exception {
        return write(createBackup());
    }

    public Backup write(Backup backup) throws Exception {
        return write(backup, createDumpQueryManager());
    }

    public Backup write(Backup backup, DumpQueryManager dumpQueryManager) throws Exception {
        boolean awaitTermination = true;
        try {
            backup.setFormat(getFormat());
            backup.setDatabaseInfo(getDatabase().getDatabaseInfo());
            Connection connection = getSession().getConnection();
            for (DumpQuery dumpQuery : getDumpQueries()) {
                backup.addRowSet(dumpQuery.getRowSet());
                while (dumpQuery.getQuerySplitter().hasNextQuerySplit(connection)) {
                    executeWork(dumpQueryManager, createWork(dumpQueryManager, dumpQuery));
                }
            }
        } catch (Throwable failure) {
            awaitTermination = false;
            throw failure instanceof MigratorException ? (MigratorException) failure : new DumpException(failure);
        } finally {
            closeDumpQueryManager(dumpQueryManager, awaitTermination);
        }
        return backup;
    }

    protected Backup createBackup() {
        return new Backup();
    }

    protected DumpQueryManager createDumpQueryManager() {
        return new SimpleDumpQueryManager();
    }

    protected Work createWork(DumpQueryManager dumpQueryManager, DumpQuery dumpQuery) throws Exception {
        DumpQueryContext dumpQueryContext = getDumpQueryContext();
        Connection connection = dumpQueryContext.getSession().getConnection();
        QuerySplitter querySplitter = dumpQuery.getQuerySplitter();
        return new DumpQueryWork(dumpQueryContext, dumpQueryManager, dumpQuery,
                querySplitter.getNextQuerySplit(connection), querySplitter.hasNextQuerySplit(connection));
    }

    protected void executeWork(final WorkManager workManager, final Work work) {
        Executor executor = dumpQueryContext.getExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Session session = null;
                try {
                    session = dumpQueryContext.getSessionFactory().openSession();
                    session.execute(work, workManager);
                } catch (Exception exception) {
                    workManager.failure(work, exception);
                } finally {
                    close(session);
                }
            }
        });
    }

    protected void closeDumpQueryManager(DumpQueryManager dumpQueryManager, boolean awaitTermination) {
        Executor executor = getExecutor();
        if (executor instanceof ExecutorService) {
            ExecutorService executorService = (ExecutorService) executor;
            executorService.shutdown();
            try {
                if (awaitTermination) {
                    executorService.awaitTermination(MAX_VALUE, SECONDS);
                }
            } catch (InterruptedException exception) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Awaiting executor's termination was interrupted", exception);
                }
            }
        }
        Map<Work, Throwable> failures = dumpQueryManager.getFailures();
        if (!isEmpty(failures)) {
            final Throwable failure = get(failures.values(), 0);
            throw failure instanceof MigratorException ? (MigratorException) failure : new DumpException(failure);
        }
    }

    protected DumpQuery createDumpQuery(String query) {
        return new DumpQuery(createQuerySplitter(query), new QueryRowSet(query));
    }

    protected QuerySplitter createQuerySplitter(String query) {
        return newNoLimitSplitter(newQuery(query));
    }

    protected DumpQuery createDumpQuery(Table table, Collection<Column> columns, String filter, QueryLimit queryLimit) {
        return new DumpTable(table, columns, filter, createQuerySplitter(table, columns, filter, queryLimit),
                new TableRowSet(table));
    }

    protected QuerySplitter createQuerySplitter(Table table, Collection<Column> columns, String filter,
                                                QueryLimit queryLimit) {
        QuerySplitter querySplitter;
        Query query = newQuery(table, columns, filter);
        Dialect dialect = getSession().getDialect();
        if (queryLimit != null && supportsLimitSplitter(dialect, table, filter)) {
            querySplitter = newLimitSplitter(dialect, newCachingStrategy(newHandlerStrategy(
                    dialect.createRowCountHandler(table, null, filter, EXACT))), query, queryLimit);
        } else {
            querySplitter = newNoLimitSplitter(query);
        }
        return querySplitter;
    }

    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(QueryLimit queryLimit) {
        this.queryLimit = queryLimit;
    }

    public Collection<DumpQuery> getDumpQueries() {
        return dumpQueries;
    }

    public void setDumpQueries(Collection<DumpQuery> dumpQueries) {
        this.dumpQueries = dumpQueries;
    }

    public DumpQueryContext getDumpQueryContext() {
        return dumpQueryContext;
    }

    public void setDumpQueryContext(DumpQueryContext dumpQueryContext) {
        this.dumpQueryContext = dumpQueryContext;
    }

    @Override
    public int getThreads() {
        return dumpQueryContext.getThreads();
    }

    @Override
    public void setThreads(int threads) {
        dumpQueryContext.setThreads(threads);
    }

    @Override
    public Database getDatabase() {
        return dumpQueryContext.getDatabase();
    }

    @Override
    public void setDatabase(Database database) {
        dumpQueryContext.setDatabase(database);
    }

    @Override
    public Executor getExecutor() {
        return dumpQueryContext.getExecutor();
    }

    @Override
    public void setExecutor(Executor executor) {
        dumpQueryContext.setExecutor(executor);
    }

    @Override
    public TimeZone getTimeZone() {
        return dumpQueryContext.getTimeZone();
    }

    @Override
    public void setTimeZone(TimeZone timeZone) {
        dumpQueryContext.setTimeZone(timeZone);
    }

    @Override
    public Session getSession() {
        return dumpQueryContext.getSession();
    }

    @Override
    public void setSession(Session session) {
        dumpQueryContext.setSession(session);
    }

    @Override
    public SessionFactory getSessionFactory() {
        return dumpQueryContext.getSessionFactory();
    }

    @Override
    public void setSessionFactory(SessionFactory sessionFactory) {
        dumpQueryContext.setSessionFactory(sessionFactory);
    }

    @Override
    public BackupManager getBackupManager() {
        return dumpQueryContext.getBackupManager();
    }

    @Override
    public void setBackupManager(BackupManager backupManager) {
        dumpQueryContext.setBackupManager(backupManager);
    }

    @Override
    public String getFormat() {
        return dumpQueryContext.getFormat();
    }

    @Override
    public void setFormat(String format) {
        dumpQueryContext.setFormat(format);
    }

    @Override
    public Map<String, Object> getFormatAttributes() {
        return dumpQueryContext.getFormatAttributes();
    }

    @Override
    public void setFormatAttributes(Map<String, Object> formatAttributes) {
        dumpQueryContext.setFormatAttributes(formatAttributes);
    }

    @Override
    public FormatFactory getFormatFactory() {
        return dumpQueryContext.getFormatFactory();
    }

    @Override
    public void setFormatFactory(FormatFactory formatFactory) {
        dumpQueryContext.setFormatFactory(formatFactory);
    }

    @Override
    public ValueFormatRegistry getValueFormatRegistry() {
        return dumpQueryContext.getValueFormatRegistry();
    }

    @Override
    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        dumpQueryContext.setValueFormatRegistry(valueFormatRegistry);
    }
}
