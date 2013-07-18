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

import com.nuodb.migrator.backup.catalog.*;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.QueryLimit;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.split.QuerySplit;
import com.nuodb.migrator.jdbc.split.QuerySplitter;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static com.nuodb.migrator.jdbc.split.Queries.newQuery;
import static com.nuodb.migrator.jdbc.split.QuerySplitters.*;
import static com.nuodb.migrator.jdbc.split.RowCountHandlerStrategies.createCachingStrategy;
import static com.nuodb.migrator.jdbc.split.RowCountHandlerStrategies.createHandlerStrategy;
import static com.nuodb.migrator.utils.CollectionUtils.isEmpty;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public class DumpWriter {

    public static final int DEFAULT_THREADS = getRuntime().availableProcessors();
    private final transient Logger logger = getLogger(getClass());

    private int threads = DEFAULT_THREADS;
    private DumpContext dumpContext;
    private QueryLimit defaultQueryLimit;
    private Collection<QueryHandle> queryHandles = newArrayList();

    public DumpWriter(DumpContext dumpContext) {
        this.dumpContext = dumpContext;
    }

    public void addTable(Table table) {
        addTable(table, table.getColumns());
    }

    public void addTable(Table table, Collection<Column> columns) {
        addTable(table, columns, null);
    }

    public void addTable(Table table, Collection<Column> columns, String filter) {
        addTable(table, columns, filter, getDefaultQueryLimit());
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
        addQuery(createQueryDesc(table, columns, filter, queryLimit));
    }

    public void addQuery(String query) {
        addQuery(createQueryDesc(query, getDefaultQueryLimit()));
    }

    protected void addQuery(QueryHandle queryHandle) {
        queryHandles.add(queryHandle);
    }

    public Catalog write() throws Exception {
        Catalog catalog = new Catalog();
        catalog.setFormat(getOutputFormat());
        catalog.setDatabaseInfo(getDatabase().getDatabaseInfo());
        write(catalog);
        return catalog;
    }

    public void write(Catalog catalog) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Constructing fixed thread pool with %d thread(s) to write dump", getThreads()));
        }
        ExecutorService executorService = newFixedThreadPool(getThreads());

        DumpQueryMonitor dumpQueryMonitor = createDumpQueryMonitor();
        try {
            write(executorService, catalog, dumpQueryMonitor);
            executorService.shutdown();
            while (!executorService.isTerminated()) {
                executorService.awaitTermination(100L, MILLISECONDS);
            }
        } catch (Exception exception) {
            executorService.shutdownNow();
            throw exception;
        } finally {
            releaseDumpQueryMonitor(dumpQueryMonitor);
        }
        getCatalogManager().writeCatalog(catalog);
    }

    protected void write(ExecutorService executorService, Catalog catalog,
                         DumpQueryMonitor dumpQueryMonitor) throws SQLException {
        RowSet rowSet;
        for (QueryHandle queryHandle : queryHandles) {
            catalog.addQueryChunkSet(rowSet = createRowSet(queryHandle));
            execute(executorService, dumpQueryMonitor, queryHandle, createQuerySplitter(queryHandle), rowSet);
        }
    }

    protected void execute(ExecutorService executorService, final DumpQueryMonitor dumpQueryMonitor,
                           QueryHandle queryHandle, QuerySplitter querySplitter, RowSet rowSet) throws SQLException {
        while (querySplitter.hasNextQuerySplit(getConnection())) {
            final DumpQuery dumpQuery = createDumpQuery(dumpQueryMonitor, queryHandle, querySplitter,
                    rowSet);
            executorService.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    execute(dumpQuery, dumpQueryMonitor);
                    return null;
                }
            });
        }
    }

    protected void execute(DumpTask dumpTask, DumpTaskMonitor dumpTaskMonitor) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Started executing %s", dumpTask));
        }
        try {
            dumpTask.init(dumpContext);
            dumpTask.execute();
        } catch (Exception exception) {
            dumpTaskMonitor.error(dumpTask, exception);
        } finally {
            dumpTask.close();
        }
    }

    protected DumpQueryMonitor createDumpQueryMonitor() {
        return new DumpWriterMonitor();
    }

    protected void releaseDumpQueryMonitor(DumpQueryMonitor dumpQueryMonitor) throws Exception {
        Map<DumpTask, Exception> errors = dumpQueryMonitor.getErrors();
        if (!isEmpty(errors)) {
            throw get(errors.values(), 0);
        }
    }

    protected QuerySplitter createQuerySplitter(QueryHandle queryHandle) {
        QuerySplitter querySplitter;
        if (queryHandle instanceof TableQueryHandle) {
            TableQueryHandle tableQueryDesc = (TableQueryHandle) queryHandle;
            querySplitter = createQuerySplitter(tableQueryDesc.getTable(), tableQueryDesc.getColumns(),
                    tableQueryDesc.getFilter(), queryHandle.getQueryLimit());
        } else {
            querySplitter = createQuerySplitter(queryHandle.getQuery());
        }
        return querySplitter;
    }

    protected RowSet createRowSet(QueryHandle queryHandle) {
        RowSet rowSet;
        if (queryHandle instanceof TableQueryHandle) {
            rowSet = new TableRowSet(((TableQueryHandle) queryHandle).getTable());
        } else {
            rowSet = new QueryRowSet(queryHandle.getQuery().toString());
        }
        return rowSet;
    }

    protected DumpQuery createDumpQuery(DumpQueryMonitor dumpQueryMonitor, QueryHandle queryHandle,
                                        QuerySplitter querySplitter, RowSet rowSet) throws SQLException {
        QuerySplit querySplit = querySplitter.getNextQuerySplit(getConnection(), new StatementCallback() {
            @Override
            public void process(Statement statement) throws SQLException {
                if (getThreads() == 1) {
                    getDialect().setStreamResults(statement, true);
                }
            }
        });
        return new DumpQuery(dumpQueryMonitor, queryHandle, querySplit, rowSet,
                querySplitter.hasNextQuerySplit(getConnection()));
    }

    protected QuerySplitter createQuerySplitter(Table table, Collection<Column> columns, String filter,
                                                QueryLimit queryLimit) {
        Query query = newQuery(table, columns, filter);
        QuerySplitter querySplitter;
        if (queryLimit != null && supportsLimitSplitter(getDialect(), table, filter)) {
            querySplitter = createLimitSplitter(getDialect(), query, queryLimit,
                    createCachingStrategy(new ReentrantLock(), createHandlerStrategy(
                            getDialect().createRowCountHandler(table, null, filter, EXACT))));
        } else {
            querySplitter = createNoLimitSplitter(query);
        }
        return querySplitter;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public QueryLimit getDefaultQueryLimit() {
        return defaultQueryLimit;
    }

    public void setDefaultQueryLimit(QueryLimit defaultQueryLimit) {
        this.defaultQueryLimit = defaultQueryLimit;
    }

    protected QueryHandle createQueryDesc(Table table, Collection<Column> columns, String filter,
                                          QueryLimit queryLimit) {
        return new TableQueryHandle(table, columns, filter, queryLimit);
    }

    protected QueryHandle createQueryDesc(String query, QueryLimit queryLimit) {
        return new QueryHandle(newQuery(query), queryLimit);
    }

    protected QuerySplitter createQuerySplitter(Query query) {
        return createNoLimitSplitter(query);
    }

    protected DumpContext getDumpContext() {
        return dumpContext;
    }

    protected Database getDatabase() {
        return dumpContext.getDatabase();
    }

    protected Dialect getDialect() {
        return dumpContext.getDialect();
    }

    protected TimeZone getTimeZone() {
        return dumpContext.getTimeZone();
    }

    protected Connection getConnection() {
        return dumpContext.getConnection();
    }

    protected FormatFactory getFormatFactory() {
        return dumpContext.getFormatFactory();
    }

    protected CatalogManager getCatalogManager() {
        return dumpContext.getCatalogManager();
    }

    protected ValueFormatRegistry getValueFormatRegistry() {
        return dumpContext.getValueFormatRegistry();
    }

    protected String getOutputFormat() {
        return dumpContext.getFormat();
    }

    protected Map<String, Object> getOutputAttributes() {
        return dumpContext.getFormatAttributes();
    }
}
