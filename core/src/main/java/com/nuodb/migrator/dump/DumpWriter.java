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

import com.nuodb.migrator.backup.catalog.Catalog;
import com.nuodb.migrator.backup.catalog.QueryRowSet;
import com.nuodb.migrator.backup.catalog.RowSet;
import com.nuodb.migrator.backup.catalog.TableRowSet;
import com.nuodb.migrator.jdbc.dialect.QueryLimit;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.session.Work;
import com.nuodb.migrator.jdbc.session.WorkManager;
import com.nuodb.migrator.jdbc.split.QuerySplit;
import com.nuodb.migrator.jdbc.split.QuerySplitter;
import com.nuodb.migrator.utils.BlockingThreadPoolExecutor;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newSessionTimeZoneSetter;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newTransactionIsolationSetter;
import static com.nuodb.migrator.jdbc.split.Queries.newQuery;
import static com.nuodb.migrator.jdbc.split.QuerySplitters.*;
import static com.nuodb.migrator.jdbc.split.RowCountStrategies.newCachingStrategy;
import static com.nuodb.migrator.jdbc.split.RowCountStrategies.newHandlerStrategy;
import static com.nuodb.migrator.utils.CollectionUtils.isEmpty;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public class DumpWriter extends DumpContext {

    private final transient Logger logger = getLogger(getClass());

    private QueryLimit queryLimit;

    private Collection<DumpWriterEntry> dumpWriterEntries = newArrayList();

    public void addTable(Table table) {
        addTable(table, table.getColumns());
    }

    public void addTable(Table table, Collection<Column> columns) {
        addTable(table, columns, null);
    }

    public void addTable(Table table, Collection<Column> columns, String filter) {
        addTable(table, columns, filter, queryLimit);
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
        QueryInfo queryInfo = createQueryInfo(table, columns, filter, queryLimit);
        QuerySplitter querySplitter = createQuerySplitter(table, columns, filter, queryLimit);
        addQuery(queryInfo, querySplitter, new TableRowSet(table));
    }

    public void addQuery(String query) {
        QueryInfo queryInfo = createQueryInfo(query, getQueryLimit());
        QuerySplitter querySplitter = createQuerySplitter(query);
        addQuery(queryInfo, querySplitter, new QueryRowSet(query));
    }

    protected void addQuery(QueryInfo queryInfo, QuerySplitter querySplitter, RowSet rowSet) {
        dumpWriterEntries.add(new DumpWriterEntry(queryInfo, querySplitter, rowSet));
    }

    public Catalog write() throws Exception {
        DumpWriterManager dumpWriterManager = createDumpWriterManager();
        try {
            ExecutorService executorService = dumpWriterManager.getExecutorService();
            for (DumpWriterEntry dumpWriterEntry : getDumpWriterEntries()) {
                QuerySplitter querySplitter = dumpWriterEntry.getQuerySplitter();
                while (querySplitter.hasNextQuerySplit(getConnection())) {
                    QuerySplit querySplit = querySplitter.getNextQuerySplit(getConnection());
                    write(dumpWriterManager, new DumpQuery(this, dumpWriterManager, dumpWriterEntry.getQueryInfo(),
                            querySplit, querySplitter.hasNextQuerySplit(getConnection()), dumpWriterEntry.getRowSet()));
                }
            }
            dumpWriterManager.getExecutorService().shutdown();
            while (!executorService.isTerminated()) {
                executorService.awaitTermination(100L, MILLISECONDS);
            }
        } catch (Exception exception) {
            dumpWriterManager.getExecutorService().shutdownNow();
            throw exception;
        } finally {
            closeDumpWriterManager(dumpWriterManager);
        }
        getCatalogManager().writeCatalog(dumpWriterManager.getCatalog());
        return dumpWriterManager.getCatalog();
    }

    protected void write(DumpWriterManager dumpWriterManager, DumpQuery dumpQuery) {
        write(dumpWriterManager.getExecutorService(), dumpWriterManager.getSessionFactory(),
                dumpWriterManager, dumpQuery);
    }

    protected void write(final ExecutorService executorService, final SessionFactory sessionFactory,
                         final WorkManager workManager, final Work work) {
        executorService.submit(new Callable() {
            @Override
            public Object call() throws Exception {
                Session session = null;
                try {
                    session = sessionFactory.openSession();
                    session.execute(work, workManager);
                } catch (Exception exception) {
                    workManager.error(work, exception);
                } finally {
                    if (session != null) {
                        session.close();
                    }
                }
                return null;
            }
        });
    }

    protected DumpWriterManager createDumpWriterManager() {
        DumpWriterManager dumpWriterManager = new DumpWriterManager();
        dumpWriterManager.setCatalog(createCatalog());
        dumpWriterManager.setExecutorService(createExecutorService());
        dumpWriterManager.setSessionFactory(createSessionFactory());
        return dumpWriterManager;
    }

    protected Catalog createCatalog() {
        Catalog catalog = new Catalog();
        catalog.setFormat(getFormat());
        catalog.setDatabaseInfo(getDatabase().getDatabaseInfo());
        for (DumpWriterEntry dumpWriterEntry : getDumpWriterEntries()) {
            catalog.addRowSet(dumpWriterEntry.getRowSet());
        }
        return catalog;
    }

    protected ExecutorService createExecutorService() {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Constructing blocking thread pool with %d thread(s) to write dump", getThreads()));
        }
        return new BlockingThreadPoolExecutor(getThreads(), 100L, MILLISECONDS);
    }

    protected SessionFactory createSessionFactory() {
        SessionFactory sessionFactory = newSessionFactory(getConnectionProvider(), getDialect());
        sessionFactory.addSessionObserver(newSessionTimeZoneSetter(getTimeZone()));
        sessionFactory.addSessionObserver(newTransactionIsolationSetter(new int[]{
                TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED
        }));
        return sessionFactory;
    }

    protected void closeDumpWriterManager(DumpWriterManager dumpWriterManager) throws Exception {
        Map<Work, Exception> errors = dumpWriterManager.getErrors();
        if (!isEmpty(errors)) {
            throw get(errors.values(), 0);
        }
    }

    protected QuerySplitter createQuerySplitter(String query) {
        return newNoLimitSplitter(newQuery(query));
    }

    protected QuerySplitter createQuerySplitter(Table table, Collection<Column> columns, String filter,
                                                QueryLimit queryLimit) {
        QuerySplitter querySplitter;
        Query query = newQuery(table, columns, filter);
        if (queryLimit != null && supportsLimitSplitter(getDialect(), table, filter)) {
            querySplitter = newLimitSplitter(getDialect(), newCachingStrategy(newHandlerStrategy(
                    getDialect().createRowCountHandler(table, null, filter, EXACT))
            ), query, queryLimit);
        } else {
            querySplitter = newNoLimitSplitter(query);
        }
        return querySplitter;
    }

    protected QueryInfo createQueryInfo(String query, QueryLimit queryLimit) {
        return new QueryInfo(newQuery(query), queryLimit);
    }

    protected QueryInfo createQueryInfo(Table table, Collection<Column> columns, String filter, QueryLimit queryLimit) {
        return new TableQueryInfo(table, columns, filter, queryLimit);
    }

    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(QueryLimit queryLimit) {
        this.queryLimit = queryLimit;
    }

    protected Collection<DumpWriterEntry> getDumpWriterEntries() {
        return dumpWriterEntries;
    }
}