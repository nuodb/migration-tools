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
import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.QueryLimit;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.session.Work;
import com.nuodb.migrator.jdbc.session.WorkManager;
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
import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.backup.format.csv.CsvAttributes.FORMAT;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newSessionTimeZoneObserver;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newTransactionIsolationObserver;
import static com.nuodb.migrator.jdbc.split.Queries.newQuery;
import static com.nuodb.migrator.jdbc.split.QuerySplitters.*;
import static com.nuodb.migrator.jdbc.split.RowCountHandlerStrategies.newCachingStrategy;
import static com.nuodb.migrator.jdbc.split.RowCountHandlerStrategies.newHandlerStrategy;
import static com.nuodb.migrator.utils.CollectionUtils.isEmpty;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public class DumpWriter implements DumpWriterContext {

    private final transient Logger logger = getLogger(getClass());
    public static final int THREADS = getRuntime().availableProcessors();

    private String format = FORMAT;
    private int threads = THREADS;
    private Dialect dialect;
    private Database database;
    private TimeZone timeZone;
    private CatalogManager catalogManager;
    private Connection connection;
    private ConnectionProvider connectionProvider;
    private FormatFactory formatFactory;
    private ValueFormatRegistry valueFormatRegistry;
    private Map<String, Object> formatAttributes = newHashMap();
    private QueryLimit queryLimit;

    private Collection<DumpQueryEntry> dumpQueryEntries = newArrayList();

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
        dumpQueryEntries.add(new DumpQueryEntry(queryInfo, querySplitter, rowSet));
    }

    public Catalog write() throws Exception {
        DumpQueryManager dumpQueryManager = new DumpQueryManager();
        dumpQueryManager.setCatalog(createCatalog());
        dumpQueryManager.setExecutorService(createExecutorService());
        dumpQueryManager.setSessionFactory(createSessionFactory());
        write(dumpQueryManager);
        return dumpQueryManager.getCatalog();
    }

    protected void write(DumpQueryManager dumpQueryManager) throws Exception {
        ExecutorService executorService = dumpQueryManager.getExecutorService();
        try {
            for (DumpQueryEntry dumpQueryEntry : dumpQueryEntries) {
                write(dumpQueryManager, dumpQueryEntry);
            }
            executorService.shutdown();
            while (!executorService.isTerminated()) {
                executorService.awaitTermination(100L, MILLISECONDS);
            }
        } catch (Exception exception) {
            executorService.shutdownNow();
            throw exception;
        } finally {
            errors(dumpQueryManager);
        }
        getCatalogManager().writeCatalog(dumpQueryManager.getCatalog());
    }

    protected void errors(DumpQueryManager dumpQueryManager) throws Exception {
        Map<Work, Exception> errors = dumpQueryManager.getErrors();
        if (!isEmpty(errors)) {
            throw get(errors.values(), 0);
        }
    }

    protected void write(DumpQueryManager dumpQueryManager, DumpQueryEntry dumpQueryEntry) throws SQLException {
        write(dumpQueryManager, dumpQueryEntry.getQueryInfo(), dumpQueryEntry.getQuerySplitter(),
                dumpQueryEntry.getRowSet());
    }

    protected void write(final DumpQueryManager dumpQueryManager, QueryInfo queryInfo, QuerySplitter querySplitter,
                         RowSet rowSet) throws SQLException {
        while (querySplitter.hasNextQuerySplit(getConnection())) {
            QuerySplit querySplit = querySplitter.getNextQuerySplit(getConnection(), new StatementCallback() {
                @Override
                public void process(Statement statement) throws SQLException {
                    getDialect().setStreamResults(statement, true);
                }
            });
            write(dumpQueryManager, new DumpQuery(this, dumpQueryManager, queryInfo, querySplit,
                    querySplitter.hasNextQuerySplit(getConnection()), rowSet));
        }
    }

    protected void write(DumpQueryManager dumpQueryManager, DumpQuery dumpQuery) {
        execute(dumpQueryManager.getExecutorService(), dumpQueryManager.getSessionFactory(),
                dumpQueryManager, dumpQuery);
    }

    protected void execute(final ExecutorService executorService, final SessionFactory sessionFactory,
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

    protected Catalog createCatalog() {
        Catalog catalog = new Catalog();
        catalog.setFormat(getFormat());
        catalog.setDatabaseInfo(getDatabase().getDatabaseInfo());
        for (DumpQueryEntry dumpQueryEntry : dumpQueryEntries) {
            catalog.addRowSet(dumpQueryEntry.getRowSet());
        }
        return catalog;
    }

    protected SessionFactory createSessionFactory() {
        SessionFactory sessionFactory = newSessionFactory(getConnectionProvider(), getDialect());
        sessionFactory.addSessionObserver(newTransactionIsolationObserver(new int[]{
                TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED
        }));
        sessionFactory.addSessionObserver(newSessionTimeZoneObserver(getTimeZone()));
        return sessionFactory;
    }

    protected ExecutorService createExecutorService() {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Constructing fixed thread pool with %d thread(s) to write dump", getThreads()));
        }
        return newFixedThreadPool(getThreads());
    }

    protected QuerySplitter createQuerySplitter(String query) {
        return newNoLimitSplitter(newQuery(query));
    }

    protected QuerySplitter createQuerySplitter(Table table, Collection<Column> columns, String filter,
                                                QueryLimit queryLimit) {
        QuerySplitter querySplitter;
        Query query = newQuery(table, columns, filter);
        if (queryLimit != null && supportsLimitSplitter(getDialect(), table, filter)) {
            querySplitter = newLimitSplitter(getDialect(),
                    newCachingStrategy(new ReentrantLock(),
                            newHandlerStrategy(
                                    getDialect().createRowCountHandler(table, null, filter, EXACT))), query, queryLimit);
        } else {
            querySplitter = newNoLimitSplitter(query);
        }
        return querySplitter;
    }

    protected QueryInfo createQueryInfo(Table table, Collection<Column> columns, String filter,
                                        QueryLimit queryLimit) {
        return new TableQueryInfo(table, columns, filter, queryLimit);
    }

    protected QueryInfo createQueryInfo(String query, QueryLimit queryLimit) {
        return new QueryInfo(newQuery(query), queryLimit);
    }

    @Override
    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    @Override
    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public void setCatalogManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    public void setConnectionProvider(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    @Override
    public ValueFormatRegistry getValueFormatRegistry() {
        return valueFormatRegistry;
    }

    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        this.valueFormatRegistry = valueFormatRegistry;
    }

    @Override
    public Map<String, Object> getFormatAttributes() {
        return formatAttributes;
    }

    public void setFormatAttributes(Map<String, Object> formatAttributes) {
        this.formatAttributes = formatAttributes;
    }

    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(QueryLimit queryLimit) {
        this.queryLimit = queryLimit;
    }
}
