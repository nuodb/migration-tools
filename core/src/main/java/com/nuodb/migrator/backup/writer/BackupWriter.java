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
import com.nuodb.migrator.backup.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.jdbc.JdbcUtils;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.HasTables;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.session.Work;
import com.nuodb.migrator.jdbc.split.QuerySplitter;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.MetaDataSpec;
import com.nuodb.migrator.spec.MigrationMode;
import com.nuodb.migrator.spec.QuerySpec;
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
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.backup.XmlMetaDataHandlerBase.META_DATA_SPEC;
import static com.nuodb.migrator.context.ContextUtils.createService;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.DATABASE;
import static com.nuodb.migrator.jdbc.query.Queries.newQuery;
import static com.nuodb.migrator.jdbc.split.QuerySplitters.*;
import static com.nuodb.migrator.jdbc.split.RowCountStrategies.newCachingStrategy;
import static com.nuodb.migrator.jdbc.split.RowCountStrategies.newHandlerStrategy;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static com.nuodb.migrator.utils.ValidationUtils.isTrue;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.ArrayUtils.indexOf;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"all"})
public class BackupWriter {

    public static final String FORMAT = CsvAttributes.FORMAT;
    public static final int THREADS = getRuntime().availableProcessors();
    public static final Collection<MigrationMode> MIGRATION_MODES = newHashSet(MigrationMode.values());
    /**
     * The default row count, which triggers write row event per each chunk
     */
    public static final Long DELTA_ROW_COUNT = null;

    protected final transient Logger logger = getLogger(getClass());

    private Collection<BackupWriterListener> listeners = newArrayList();
    private Database database;
    private Executor executor;
    /**
     * Delta row count per chunk that triggers write row event, null means that write row event is not generated
     */
    private Long deltaRowCount = DELTA_ROW_COUNT;
    private InspectionManager inspectionManager;
    private String format = FORMAT;
    private Map<String, Object> formatAttributes = newHashMap();
    private FormatFactory formatFactory;
    private MetaDataSpec metaDataSpec;
    private Collection<MigrationMode> migrationModes = MIGRATION_MODES;
    private QueryLimit queryLimit;
    private Collection<QuerySpec> querySpecs;
    private ConnectionSpec sourceSpec;
    private SessionFactory sourceSessionFactory;
    private TimeZone timeZone;
    private Integer threads = THREADS;
    private ValueFormatRegistryResolver valueFormatRegistryResolver;
    private Collection<WriteRowSet> writeRowSets = newArrayList();

    public void addQuery(String query) {
        addWriteRowSet(createWriteRowSet(query), getWriteRowSets());
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
    public void addTable(Table table, Collection<Column> columns, String filter,
                         QueryLimit queryLimit) {
        addTable(table, columns, filter, queryLimit, getWriteRowSets());
    }

    protected void addTable(Table table, Collection<Column> columns, String filter,
                            QueryLimit queryLimit, Collection<WriteRowSet> writeRowSets) {
        addWriteRowSet(createWriteRowSet(table, columns, filter, queryLimit), writeRowSets);
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
        addTables(tables, tableTypes, getWriteRowSets());
    }

    protected void addTables(HasTables tables, String[] tableTypes,
                             Collection<WriteRowSet> writeRowSets) {
        for (Table table : tables.getTables()) {
            if (isEmpty(tableTypes) || indexOf(tableTypes, table.getType()) != -1) {
                addWriteRowSet(createWriteRowSet(
                        table, table.getColumns(), null, getQueryLimit()), writeRowSets);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(format("Table %s %s is not in the allowed types, table skipped",
                            table.getQualifiedName(null), table.getType()));
                }
            }
        }
    }

    protected void addWriteRowSet(WriteRowSet writeRowSet, Collection<WriteRowSet> writeRowSets) {
        writeRowSets.add(writeRowSet);
    }

    public void addTable(Table table) {
        addTable(table, table.getColumns());
    }

    public void addListener(BackupWriterListener listener) {
        listeners.add(listener);
    }

    public void removeListener(BackupWriterListener listener) {
        listeners.remove(listener);
    }

    public BackupWriterListener[] getListeners() {
        int size = listeners.size();
        return listeners.toArray(new BackupWriterListener[size]);
    }

    public Backup write(String path) throws Exception {
        return write(path, newHashMap());
    }

    public Backup write(String path, Map context) throws Exception {
        return write(createBackupOps(path), context);
    }

    protected Backup write(BackupOps backupOps, Map context) throws Exception {
        return write(createBackupWriterManager((BackupOps) backupOps, (Map) context));
    }

    protected BackupOps createBackupOps(String path) {
        BackupOps backupOps = createService(BackupOps.class);
        backupOps.setPath(path);
        return backupOps;
    }

    protected BackupWriterContext createBackupWriterContext(BackupOps backupOps, Map context) throws Exception {
        BackupWriterContext backupWriterContext = new SimpleBackupWriterContext();
        backupWriterContext.setBackup(createBackup());
        backupWriterContext.setBackupOps(backupOps);
        backupWriterContext.setBackupOpsContext(context);

        Executor executor = getExecutor();
        backupWriterContext.setExecutor(executor == null ? createExecutor() : executor);
        backupWriterContext.setFormat(getFormat());
        backupWriterContext.setFormatAttributes(getFormatAttributes());
        backupWriterContext.setFormatFactory(getFormatFactory());
        backupWriterContext.setMigrationModes(getMigrationModes());
        backupWriterContext.setThreads(getThreads());
        backupWriterContext.setTimeZone(getTimeZone());
        openSourceSession(backupWriterContext);
        return backupWriterContext;
    }

    protected BackupWriterManager createBackupWriterManager(BackupOps backupOps, Map context) throws Exception {
        BackupWriterContext backupWriterContext = createBackupWriterContext(backupOps, context);
        return createBackupWriterManager(backupWriterContext);
    }

    protected Backup createBackup() {
        Backup backup = new Backup();
        backup.setFormat(getFormat());
        return backup;
    }

    protected BackupWriterManager createBackupWriterManager(BackupWriterContext backupWriterContext) {
        BackupWriterManager backupWriterManager = new SimpleBackupWriterManager();
        backupWriterManager.setDeltaRowCount(getDeltaRowCount());
        backupWriterManager.setBackupWriterContext(backupWriterContext);
        for (BackupWriterListener listener : getListeners()) {
            backupWriterManager.addListener(listener);
        }
        return backupWriterManager;
    }

    protected Executor createExecutor() {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Using blocking thread pool with %d thread(s)", getThreads()));
        }
        return new BlockingThreadPoolExecutor(getThreads(), 100L, MILLISECONDS);
    }

    protected ValueFormatRegistry createValueFormatRegistry(Session session) throws Exception {
        return getValueFormatRegistryResolver().resolve(session);
    }

    protected void openSourceSession(BackupWriterContext backupWriterContext) throws Exception {
        SessionFactory sourceSessionFactory = getSourceSessionFactory();
        Session sourceSession = sourceSessionFactory.openSession();
        backupWriterContext.setSourceSessionFactory(sourceSessionFactory);
        backupWriterContext.setSourceSession(sourceSession);
        try {
            backupWriterContext.setValueFormatRegistry(
                    createValueFormatRegistry(sourceSession));
            final Database database = getDatabase();
            backupWriterContext.setDatabase(database == null ?
                    openDatabase(sourceSession) : database);
        } catch (Exception exception) {
            JdbcUtils.closeQuietly(sourceSession);
            throw exception;
        }
    }

    protected Database openDatabase(Session session) throws Exception {
        ConnectionSpec connectionSpec = session.getConnectionSpec();
        InspectionScope inspectionScope = new TableInspectionScope(
                connectionSpec.getCatalog(), connectionSpec.getSchema(), getTableTypes());
        return getInspectionManager().inspect(session.getConnection(), inspectionScope,
                getObjectTypes().toArray(new MetaDataType[0])).getObject(DATABASE);
    }

    protected Backup write(BackupWriterManager backupWriterManager) throws Exception {
        try {
            if (backupWriterManager.isWriteData()) {
                writeData(backupWriterManager);
            }
            if (backupWriterManager.isWriteSchema()) {
                writeSchema(backupWriterManager);
            }
            writeBackup(backupWriterManager);
        } catch (Throwable failure) {
            backupWriterManager.writeFailed();
            throw failure instanceof MigratorException ?
                    (MigratorException) failure : new BackupWriterException(failure);
        } finally {
            backupWriterManager.close();
        }
        return backupWriterManager.getBackupWriterContext().getBackup();
    }

    protected void writeData(BackupWriterManager backupWriterManager) throws Exception {
        BackupWriterContext backupWriterContext = backupWriterManager.getBackupWriterContext();
        for (WriteRowSet writeRowSet : createWriteRowSets(backupWriterContext)) {
            writeData(writeRowSet, backupWriterManager);
        }
        backupWriterManager.writeDataDone();
    }

    protected void writeData(WriteRowSet writeRowSet, BackupWriterManager backupWriterManager) throws Exception {
        BackupWriterContext backupWriterContext = backupWriterManager.getBackupWriterContext();
        Backup backup = backupWriterContext.getBackup();
        Session session = backupWriterContext.getSourceSession();
        backup.addRowSet(writeRowSet.getRowSet());
        while (writeRowSet.getQuerySplitter().hasNextQuerySplit(session.getConnection())) {
            Work work = createWork(writeRowSet, backupWriterManager);
            executeWork(work, backupWriterManager);
        }
        backupWriterManager.writeDataDone();
    }

    protected void writeSchema(BackupWriterManager backupWriterManager) throws Exception {
        BackupWriterContext backupWriterContext = backupWriterManager.getBackupWriterContext();
        Backup backup = backupWriterContext.getBackup();
        backup.setDatabase(backupWriterContext.getDatabase());
        backupWriterManager.writeSchemaDone();
    }

    protected void writeBackup(BackupWriterManager backupWriterManager) throws Exception {
        BackupWriterContext backupWriterContext = backupWriterManager.getBackupWriterContext();
        Backup backup = backupWriterContext.getBackup();
        Map backupOpsContext = newHashMap(backupWriterContext.getBackupOpsContext());
        backupOpsContext.put(META_DATA_SPEC, getMetaDataSpec());
        backupWriterContext.getBackupOps().write(backup, backupOpsContext);
    }

    protected Work createWork(WriteRowSet writeRowSet, BackupWriterManager backupWriterManager) throws Exception {
        BackupWriterContext backupWriterContext = backupWriterManager.getBackupWriterContext();
        Connection connection = backupWriterContext.getSourceSession().getConnection();
        QuerySplitter querySplitter = writeRowSet.getQuerySplitter();
        return new WriteRowSetWork(writeRowSet, querySplitter.getNextQuerySplit(connection),
                querySplitter.hasNextQuerySplit(connection), backupWriterManager
        );
    }

    protected void executeWork(final Work work, final BackupWriterManager backupWriterManager) {
        final BackupWriterContext backupWriterContext = backupWriterManager.getBackupWriterContext();
        backupWriterContext.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                backupWriterManager.execute(work,
                        backupWriterContext.getSourceSessionFactory());
            }
        });
    }

    protected Collection<WriteRowSet> createWriteRowSets(BackupWriterContext backupWriterContext) {
        Collection<WriteRowSet> exportQueries = newArrayList(getWriteRowSets());
        Database database = backupWriterContext.getDatabase();
        Collection<TableSpec> tableSpecs = getTableSpecs();
        if (isEmpty(tableSpecs)) {
            addTables(database, getTableTypes(), exportQueries);
        } else {
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
                exportQueries.add(createWriteRowSet(table, columns, filter, getQueryLimit()));
            }
        }
        Collection<QuerySpec> querySpecs = getQuerySpecs();
        if (!isEmpty(querySpecs)) {
            for (QuerySpec querySpec : querySpecs) {
                addWriteRowSet(createWriteRowSet(querySpec.getQuery()), exportQueries);
            }
        }
        return exportQueries;
    }

    protected WriteRowSet createWriteRowSet(String query) {
        return new WriteRowSet(createQuerySplitter(query), new QueryRowSet(query));
    }

    protected QuerySplitter createQuerySplitter(String query) {
        return newNoLimitSplitter(newQuery(query));
    }

    protected WriteRowSet createWriteRowSet(Table table, Collection<Column> columns, String filter,
                                            QueryLimit queryLimit) {
        return new WriteTableRowSet(table, columns, filter,
                createQuerySplitter(table, columns, filter, queryLimit), new TableRowSet(table));
    }

    protected QuerySplitter createQuerySplitter(Table table, Collection<Column> columns, String filter,
                                                QueryLimit queryLimit) {
        QuerySplitter querySplitter;
        Query query = newQuery(table, columns, filter);
        Dialect dialect = table.getDatabase().getDialect();
        if (queryLimit != null && supportsLimitSplitter(dialect, table, filter)) {
            querySplitter = newLimitSplitter(dialect, newCachingStrategy(newHandlerStrategy(
                    dialect.createRowCountHandler(table, null, filter, EXACT))), query, queryLimit);
        } else {
            querySplitter = newNoLimitSplitter(query);
        }
        return querySplitter;
    }

    protected Collection<MetaDataType> getObjectTypes() {
        final MetaDataSpec metaDataSpec = getMetaDataSpec();
        return metaDataSpec != null ? metaDataSpec.getObjectTypes() : null;
    }

    protected String[] getTableTypes() {
        final MetaDataSpec metaDataSpec = getMetaDataSpec();
        return metaDataSpec != null ? metaDataSpec.getTableTypes() : null;
    }

    protected Collection<TableSpec> getTableSpecs() {
        final MetaDataSpec metaDataSpec = getMetaDataSpec();
        return metaDataSpec != null ? metaDataSpec.getTableSpecs() : null;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public Long getDeltaRowCount() {
        return deltaRowCount;
    }

    public void setDeltaRowCount(Long deltaRowCount) {
        isTrue(deltaRowCount > 0, "Delta row count should be greater than 0");
        this.deltaRowCount = deltaRowCount;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    protected Collection<WriteRowSet> getWriteRowSets() {
        return writeRowSets;
    }

    protected void setWriteRowSets(Collection<WriteRowSet> writeRowSets) {
        this.writeRowSets = writeRowSets;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format != null ? format : FORMAT;
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

    public InspectionManager getInspectionManager() {
        return inspectionManager;
    }

    public void setInspectionManager(InspectionManager inspectionManager) {
        this.inspectionManager = inspectionManager;
    }

    public MetaDataSpec getMetaDataSpec() {
        return metaDataSpec;
    }

    public void setMetaDataSpec(MetaDataSpec metaDataSpec) {
        this.metaDataSpec = metaDataSpec;
    }

    public Collection<MigrationMode> getMigrationModes() {
        return migrationModes;
    }

    public void setMigrationModes(Collection<MigrationMode> migrationModes) {
        this.migrationModes = migrationModes;
    }

    public QueryLimit getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(QueryLimit queryLimit) {
        this.queryLimit = queryLimit;
    }

    public Collection<QuerySpec> getQuerySpecs() {
        return querySpecs;
    }

    public void setQuerySpecs(Collection<QuerySpec> querySpecs) {
        this.querySpecs = querySpecs;
    }

    public ConnectionSpec getSourceSpec() {
        return sourceSpec;
    }

    public void setSourceSpec(ConnectionSpec sourceSpec) {
        this.sourceSpec = sourceSpec;
    }

    public SessionFactory getSourceSessionFactory() {
        return sourceSessionFactory;
    }

    public void setSourceSessionFactory(SessionFactory sourceSessionFactory) {
        this.sourceSessionFactory = sourceSessionFactory;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public Integer getThreads() {
        return threads;
    }

    public void setThreads(Integer threads) {
        this.threads = threads;
        this.executor = null;
    }

    public ValueFormatRegistryResolver getValueFormatRegistryResolver() {
        return valueFormatRegistryResolver;
    }

    public void setValueFormatRegistryResolver(ValueFormatRegistryResolver valueFormatRegistryResolver) {
        this.valueFormatRegistryResolver = valueFormatRegistryResolver;
    }
}
