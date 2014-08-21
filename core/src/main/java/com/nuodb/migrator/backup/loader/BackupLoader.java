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
package com.nuodb.migrator.backup.loader;

import com.google.common.base.Function;
import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.Backup;
import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.backup.Column;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.TableRowSet;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.jdbc.commit.CommitStrategy;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.dialect.TranslationConfig;
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.generator.CompositeScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ProxyScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;
import com.nuodb.migrator.jdbc.metadata.generator.SessionScriptExporter;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.query.InsertQueryBuilder;
import com.nuodb.migrator.jdbc.query.InsertType;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.session.Work;
import com.nuodb.migrator.jdbc.type.JdbcTypeNameMap;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.JdbcTypeSpec;
import com.nuodb.migrator.spec.MetaDataSpec;
import com.nuodb.migrator.spec.MigrationMode;
import com.nuodb.migrator.utils.concurrent.BlockingThreadPoolExecutor;
import com.nuodb.migrator.utils.PrioritySet;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.context.ContextUtils.createService;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.NUODB;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migrator.jdbc.metadata.generator.HasTablesScriptGenerator.GROUP_SCRIPTS_BY;
import static com.nuodb.migrator.jdbc.query.InsertType.INSERT;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;
import static com.nuodb.migrator.utils.Collections.*;
import static com.nuodb.migrator.utils.ValidationUtils.isTrue;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class BackupLoader {

    public static final Collection<MigrationMode> MIGRATION_MODES = newHashSet(MigrationMode.values());
    public static final int THREADS = getRuntime().availableProcessors();
    public static final Long DELTA_ROW_COUNT = null;

    protected final transient Logger logger = getLogger(getClass());

    private CommitStrategy commitStrategy;
    private Database database;
    private Long deltaRowCount = DELTA_ROW_COUNT;
    private DialectResolver dialectResolver;
    private Executor executor;
    private FormatFactory formatFactory;
    private Map<String, Object> formatAttributes = newHashMap();
    private GroupScriptsBy groupScriptsBy;
    private Collection<JdbcTypeSpec> jdbcTypeSpecs;
    private Collection<BackupLoaderListener> listeners = newArrayList();
    private IdentifierQuoting identifierQuoting;
    private IdentifierNormalizer identifierNormalizer;
    private InsertTypeFactory insertTypeFactory;
    private InspectionManager inspectionManager;
    private MetaDataSpec metaDataSpec;
    private Collection<MigrationMode> migrationModes = MIGRATION_MODES;
    private PrioritySet<NamingStrategy> namingStrategies;
    private RowSetMapper rowSetMapper = new SimpleRowSetMapper();
    private Collection<ScriptType> scriptTypes;
    private ConnectionSpec targetSpec;
    private SessionFactory targetSessionFactory;
    private TimeZone timeZone;
    private TranslationConfig translationConfig;
    private int threads = THREADS;
    private ScriptExporter scriptExporter;
    private ValueFormatRegistryResolver valueFormatRegistryResolver;

    public Backup load(String path) throws Exception {
        return load(path, newHashMap());
    }

    public Backup load(String path, Map context) throws Exception {
        return load(createBackupOps(path), context);
    }

    protected BackupOps createBackupOps(String path) {
        BackupOps backupOps = createService(BackupOps.class);
        backupOps.setPath(path);
        return backupOps;
    }

    protected Backup load(BackupOps backupOps, Map context) throws Exception {
        return load(createBackupLoaderManager(backupOps, context));
    }

    protected BackupLoaderManager createBackupLoaderManager(BackupOps backupOps, Map context) throws Exception {
        BackupLoaderContext backupLoaderContext = createBackupLoaderContext(backupOps, context);
        return createBackupLoaderManager(backupLoaderContext);
    }

    protected BackupLoaderContext createBackupLoaderContext(BackupOps backupOps, Map context) throws Exception {
        BackupLoaderContext backupLoaderContext = new SimpleBackupLoaderContext();
        backupLoaderContext.setBackup(backupOps.read(context));
        backupLoaderContext.setBackupOps(backupOps);
        backupLoaderContext.setBackupOpsContext(context);
        backupLoaderContext.setCommitStrategy(getCommitStrategy());

        Executor executor = getExecutor();
        backupLoaderContext.setExecutor(executor == null ? createExecutor() : executor);
        backupLoaderContext.setFormatAttributes(getFormatAttributes());
        backupLoaderContext.setFormatFactory(getFormatFactory());
        backupLoaderContext.setInsertTypeFactory(getInsertTypeFactory());
        backupLoaderContext.setMigrationModes(getMigrationModes());
        backupLoaderContext.setRowSetMapper(getRowSetMapper());
        backupLoaderContext.setTimeZone(getTimeZone());
        if (backupLoaderContext.isLoadSchema()) {
            backupLoaderContext.setLoadConstraints(createLoadConstraints(backupLoaderContext));
        }
        openSourceSession(backupLoaderContext);
        openTargetSession(backupLoaderContext);
        return backupLoaderContext;
    }

    protected BackupLoaderManager createBackupLoaderManager(final BackupLoaderContext backupLoaderContext) {
        BackupLoaderManager backupLoaderManager = new SimpleBackupLoaderManager();
        backupLoaderManager.setBackupLoaderContext(backupLoaderContext);
        // add listener after load constraints is created
        if (backupLoaderManager.isLoadSchema()) {
            backupLoaderManager.addListener(
                    new LoadConstraintListener(this, backupLoaderManager));
        }
        for (BackupLoaderListener listener : getListeners()) {
            backupLoaderManager.addListener(listener);
        }
        return backupLoaderManager;
    }

    protected Executor createExecutor() {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Using blocking thread pool with %d thread(s)", getThreads()));
        }
        return new BlockingThreadPoolExecutor(getThreads(), 100L, MILLISECONDS);
    }

    protected void openSourceSession(BackupLoaderContext backupLoaderContext) throws SQLException {
        Database database = backupLoaderContext.getBackup().getDatabase();
        SessionFactory sourceSessionFactory = newSessionFactory(database.getDialect(), database.getConnectionSpec());
        backupLoaderContext.setSourceSessionFactory(sourceSessionFactory);
        Session sourceSession = sourceSessionFactory.openSession();
        backupLoaderContext.setSourceSession(sourceSession);
        ConnectionSpec sourceSpec = sourceSession.getConnectionSpec();
        backupLoaderContext.setSourceSpec(sourceSpec);
    }

    protected void openTargetSession(BackupLoaderContext backupLoaderContext) throws Exception {
        SessionFactory targetSessionFactory = getTargetSessionFactory();
        backupLoaderContext.setTargetSessionFactory(targetSessionFactory);
        Session targetSession = targetSessionFactory.openSession();
        backupLoaderContext.setTargetSession(targetSession);
        backupLoaderContext.setTargetSpec(getTargetSpec());
        try {
            backupLoaderContext.setScriptGeneratorManager(
                    createScriptGeneratorManager(backupLoaderContext));
            backupLoaderContext.setValueFormatRegistry(
                    createValueFormatRegistry(targetSession));
        } catch (Exception exception) {
            closeQuietly(targetSession);
            throw exception;
        }
    }

    protected Database openDatabase(Session session) throws SQLException {
        InspectionScope inspectionScope = new TableInspectionScope(null, null, getTableTypes());
        return getInspectionManager().inspect(session.getConnection(), inspectionScope,
                DATABASE, CATALOG, MetaDataType.SCHEMA, TABLE, COLUMN).getObject(DATABASE);
    }

    protected ValueFormatRegistry createValueFormatRegistry(Session session) throws Exception {
        return getValueFormatRegistryResolver().resolve(session);
    }

    protected ScriptExporter createScriptExporter(BackupLoaderContext backupLoaderContext) throws Exception {
        Collection<ScriptExporter> scriptExporters = newArrayList();
        ScriptExporter scriptExporter = getScriptExporter();
        if (scriptExporter != null) {
            // will close underlying script exporter later manually
            scriptExporters.add(new ProxyScriptExporter(scriptExporter, false));
        }
        SessionFactory targetSessionFactory = backupLoaderContext.getTargetSessionFactory();
        if (targetSessionFactory != null) {
            scriptExporters.add(new SessionScriptExporter(targetSessionFactory.openSession()));
        }
        return new CompositeScriptExporter(scriptExporters);
    }

    protected ScriptGeneratorManager createScriptGeneratorManager(
            BackupLoaderContext backupLoaderContext) throws SQLException {
        ScriptGeneratorManager scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.getAttributes().put(GROUP_SCRIPTS_BY, getGroupScriptsBy());
        scriptGeneratorManager.setObjectTypes(getObjectTypes());
        scriptGeneratorManager.setScriptTypes(getScriptTypes());

        Session sourceSession = backupLoaderContext.getSourceSession();
        ConnectionSpec sourceSpec = sourceSession != null ? sourceSession.getConnectionSpec() : null;
        scriptGeneratorManager.setSourceCatalog(sourceSpec != null ? sourceSpec.getCatalog() : null);
        scriptGeneratorManager.setSourceSchema(sourceSpec != null ? sourceSpec.getSchema() : null);
        scriptGeneratorManager.setSourceSession(sourceSession);

        ConnectionSpec targetSpec = backupLoaderContext.getTargetSpec();
        if (targetSpec != null) {
            scriptGeneratorManager.setTargetCatalog(targetSpec.getCatalog());
            scriptGeneratorManager.setTargetSchema(targetSpec.getSchema());
        }

        DialectResolver dialectResolver = getDialectResolver();
        Session targetSession = backupLoaderContext.getTargetSession();
        Dialect dialect = targetSession != null ? dialectResolver.resolve(
                targetSession.getConnection()) : dialectResolver.resolve(NUODB);

        dialect.getTranslationManager().setTranslationConfig(getTranslationConfig());
        JdbcTypeNameMap jdbcTypeNameMap = dialect.getJdbcTypeNameMap();
        Collection<JdbcTypeSpec> jdbcTypeSpecs = getJdbcTypeSpecs();
        if (jdbcTypeSpecs != null) {
            for (JdbcTypeSpec jdbcTypeSpec : jdbcTypeSpecs) {
                jdbcTypeNameMap.addJdbcTypeName(
                        jdbcTypeSpec.getTypeCode(), newOptions(
                        jdbcTypeSpec.getSize(), jdbcTypeSpec.getPrecision(), jdbcTypeSpec.getScale()),
                        jdbcTypeSpec.getTypeName()
                );
            }
        }
        IdentifierQuoting identifierQuoting = getIdentifierQuoting();
        if (identifierQuoting != null) {
            dialect.setIdentifierQuoting(identifierQuoting);
        }
        IdentifierNormalizer identifierNormalizer = getIdentifierNormalizer();
        if (identifierNormalizer != null) {
            dialect.setIdentifierNormalizer(identifierNormalizer);
        }
        PrioritySet<NamingStrategy> namingStrategies = getNamingStrategies();
        if (!isEmpty(namingStrategies)) {
            for (PrioritySet.Entry<NamingStrategy> entry : namingStrategies.entries()) {
                scriptGeneratorManager.addNamingStrategy(entry.getValue(), entry.getPriority());
            }
        }
        scriptGeneratorManager.setTargetDialect(dialect);
        return scriptGeneratorManager;
    }

    protected Backup load(BackupLoaderManager backupLoaderManager) throws Exception {
        try {
            if (backupLoaderManager.isLoadSchema()) {
                loadNoConstraints(backupLoaderManager);
            }
            if (backupLoaderManager.isLoadData()) {
                loadData(backupLoaderManager);
            }
            if (backupLoaderManager.isLoadSchema()) {
                loadConstraints(backupLoaderManager);
            }
        } catch (Throwable failure) {
            backupLoaderManager.loadFailed();
            throw failure instanceof MigratorException ?
                    (MigratorException) failure : new BackupLoaderException(failure);
        } finally {
            backupLoaderManager.close();
        }
        return backupLoaderManager.getBackupLoaderContext().getBackup();
    }

    protected void loadNoConstraints(BackupLoaderManager backupLoaderManager) throws Exception {
        BackupLoaderContext backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        ScriptGeneratorManager scriptGeneratorManager =
                backupLoaderContext.getScriptGeneratorManager();
        Collection<MetaDataType> objectTypes = getObjectTypes();
        ScriptExporter scriptExporter = createScriptExporter(backupLoaderContext);
        try {
            scriptExporter.open();
            scriptGeneratorManager.setObjectTypes(
                    removeAll(newArrayList(objectTypes),
                            newArrayList(PRIMARY_KEY, FOREIGN_KEY, INDEX)));
            scriptExporter.exportScripts(
                    scriptGeneratorManager.getScripts(
                            backupLoaderContext.getBackup().getDatabase()));
        } finally {
            closeQuietly(scriptExporter);
            scriptGeneratorManager.setObjectTypes(objectTypes);
        }
        backupLoaderManager.loadNoConstraintsDone();
    }

    /**
     * Multi-threaded data load on table level in place, continue with row level
     *
     * @param backupLoaderManager to manage this load
     * @throws Exception if data loading caused error
     */
    protected void loadData(BackupLoaderManager backupLoaderManager) throws Exception {
        BackupLoaderContext backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        Database database = getDatabase();
        backupLoaderContext.setDatabase(database != null ? database :
                openDatabase(backupLoaderContext.getTargetSession()));
        backupLoaderContext.setLoadRowSets(createLoadRowSets(backupLoaderContext));
        for (LoadRowSet loadRowSet : backupLoaderContext.getLoadRowSets()) {
            loadData(loadRowSet, backupLoaderManager);
        }
        backupLoaderManager.loadDataDone();
    }

    /**
     * Load constraints for source tables without row sets
     *
     * @param backupLoaderManager to manage this load
     * @throws Exception if constraints loading caused error
     */
    protected void loadConstraints(BackupLoaderManager backupLoaderManager) throws Exception {
        BackupLoaderContext backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        LoadConstraints loadConstraints = backupLoaderContext.getLoadConstraints();
        LoadConstraints loadConstraintsNow = new LoadConstraints(
                loadConstraints.getLoadConstraints(INDEX, PRIMARY_KEY));
        LoadRowSets loadRowSets = backupLoaderContext.getLoadRowSets();
        if (!isEmpty(loadRowSets)) {
            for (LoadRowSet loadRowSet : loadRowSets) {
                Table table = getTable(loadRowSet, backupLoaderContext);
                if (table != null) {
                    loadConstraintsNow.getLoadConstraints().removeAll(table);
                }
            }
        }
        for (LoadConstraint loadConstraint : loadConstraintsNow) {
            loadConstraint(loadConstraint, backupLoaderManager);
        }
        if (isEmpty(loadConstraints)) {
            backupLoaderManager.loadConstraintsDone();
        }
    }

    protected void loadConstraint(LoadConstraint loadConstraint, BackupLoaderManager backupLoaderManager) {
        Work work = createWork(loadConstraint, backupLoaderManager);
        executeWork(work, backupLoaderManager);
    }

    protected void loadConstraints(Collection<LoadConstraint> loadConstraints, BackupLoaderManager backupLoaderManager) {
        if (!isEmpty(loadConstraints)) {
            for (LoadConstraint loadConstraint : loadConstraints) {
                loadConstraint(loadConstraint, backupLoaderManager);
            }
        }
    }

    protected LoadConstraints createLoadConstraints(BackupLoaderContext backupLoaderContext) {
        LoadConstraints loadConstraints = new LoadConstraints();
        boolean loadIndex = contains(getObjectTypes(), INDEX);
        boolean loadPrimaryKey = contains(getObjectTypes(), PRIMARY_KEY);
        boolean loadForeignKey = contains(getObjectTypes(), FOREIGN_KEY);
        Database database = backupLoaderContext.getBackup().getDatabase();
        for (Table table : database.getTables()) {
            if (loadIndex) {
                for (Index index : table.getIndexes()) {
                    if (!index.isPrimary()) {
                        loadConstraints.putIndex(index);
                    }
                }
            }
            if (loadPrimaryKey) {
                PrimaryKey primaryKey = table.getPrimaryKey();
                if (primaryKey != null) {
                    loadConstraints.putPrimaryKey(primaryKey);
                }
            }
            if (loadForeignKey) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    loadConstraints.putForeignKey(foreignKey);
                }
            }
        }
        return loadConstraints;
    }

    protected Work createWork(LoadConstraint loadConstraint, BackupLoaderManager backupLoaderManager) {
        return new LoadConstraintWork(loadConstraint, backupLoaderManager);
    }

    protected LoadRowSets createLoadRowSets(BackupLoaderContext backupLoaderContext) {
        LoadRowSets loadRowSets = new LoadRowSets();
        for (RowSet rowSet : backupLoaderContext.getBackup().getRowSets()) {
            if (isEmpty(rowSet.getChunks())) {
                continue;
            }
            Table table = backupLoaderContext.getRowSetMapper().
                    mapRowSet(rowSet, backupLoaderContext);
            if (table == null) {
                continue;
            }
            Query query = createQuery(rowSet, table, backupLoaderContext);
            loadRowSets.addLoadRowSet(new LoadRowSet(rowSet, table, query));
        }
        return loadRowSets;
    }

    protected void loadData(LoadRowSet loadRowSet, BackupLoaderManager backupLoaderManager) {
        Work work = createWork(loadRowSet, backupLoaderManager);
        executeWork(work, backupLoaderManager);
    }

    protected Work createWork(LoadRowSet loadRowSet, BackupLoaderManager backupLoaderManager) {
        return new LoadRowSetWork(loadRowSet, backupLoaderManager);
    }

    protected void executeWork(final Work work, final BackupLoaderManager backupLoaderManager) {
        final BackupLoaderContext backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        backupLoaderContext.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                backupLoaderManager.execute(work,
                        backupLoaderContext.getTargetSessionFactory());
            }
        });
    }

    /**
     * Looks up source table meta data for a given load row set
     *
     * @param loadRowSet          to look up source table for
     * @param backupLoaderContext evaluation context
     * @return source table
     */
    protected Table getTable(LoadRowSet loadRowSet, BackupLoaderContext backupLoaderContext) {
        Table table = null;
        Database database = backupLoaderContext.getBackup().getDatabase();
        if (loadRowSet.getRowSet() instanceof TableRowSet) {
            TableRowSet tableRowSet = (TableRowSet) loadRowSet.getRowSet();
            Catalog catalog = database.hasCatalog(tableRowSet.getCatalog()) ?
                    database.getCatalog(tableRowSet.getCatalog()) : null;
            Schema schema = catalog != null && catalog.hasSchema(tableRowSet.getSchema()) ?
                    catalog.getSchema(tableRowSet.getSchema()) : null;
            table = schema != null && schema.hasTable(tableRowSet.getTable()) ?
                    schema.getTable(tableRowSet.getTable()) : null;
        }
        return table;
    }

    protected Query createQuery(RowSet rowSet, Table table, BackupLoaderContext backupLoaderContext) {
        InsertTypeFactory insertTypeFactory = backupLoaderContext.getInsertTypeFactory();
        InsertType insertType = insertTypeFactory != null ?
                insertTypeFactory.createInsertType(table, backupLoaderContext) : INSERT;
        InsertQueryBuilder builder = new InsertQueryBuilder();
        builder.insertType(insertType).into(table);
        builder.columns(newArrayList(transform(rowSet.getColumns(),
                new Function<Column, String>() {
                    @Override
                    public String apply(Column column) {
                        return column.getName();
                    }
                })));
        return builder.build();
    }

    protected Collection<MetaDataType> getObjectTypes() {
        final MetaDataSpec metaDataSpec = getMetaDataSpec();
        return metaDataSpec != null ? metaDataSpec.getObjectTypes() : null;
    }

    protected String[] getTableTypes() {
        final MetaDataSpec metaDataSpec = getMetaDataSpec();
        return metaDataSpec != null ? metaDataSpec.getTableTypes() : null;
    }

    public CommitStrategy getCommitStrategy() {
        return commitStrategy;
    }

    public void setCommitStrategy(CommitStrategy commitStrategy) {
        this.commitStrategy = commitStrategy;
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

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    public Map<String, Object> getFormatAttributes() {
        return formatAttributes;
    }

    public void setFormatAttributes(Map<String, Object> formatAttributes) {
        this.formatAttributes = formatAttributes;
    }

    public GroupScriptsBy getGroupScriptsBy() {
        return groupScriptsBy;
    }

    public void setGroupScriptsBy(GroupScriptsBy groupScriptsBy) {
        this.groupScriptsBy = groupScriptsBy;
    }

    public Collection<JdbcTypeSpec> getJdbcTypeSpecs() {
        return jdbcTypeSpecs;
    }

    public void setJdbcTypeSpecs(Collection<JdbcTypeSpec> jdbcTypeSpecs) {
        this.jdbcTypeSpecs = jdbcTypeSpecs;
    }

    public PrioritySet<NamingStrategy> getNamingStrategies() {
        return namingStrategies;
    }

    public void setNamingStrategies(PrioritySet<NamingStrategy> namingStrategies) {
        this.namingStrategies = namingStrategies;
    }

    public IdentifierQuoting getIdentifierQuoting() {
        return identifierQuoting;
    }

    public void setIdentifierQuoting(IdentifierQuoting identifierQuoting) {
        this.identifierQuoting = identifierQuoting;
    }

    public IdentifierNormalizer getIdentifierNormalizer() {
        return identifierNormalizer;
    }

    public void setIdentifierNormalizer(IdentifierNormalizer identifierNormalizer) {
        this.identifierNormalizer = identifierNormalizer;
    }

    public InsertTypeFactory getInsertTypeFactory() {
        return insertTypeFactory;
    }

    public void setInsertTypeFactory(InsertTypeFactory insertTypeFactory) {
        this.insertTypeFactory = insertTypeFactory;
    }

    public InspectionManager getInspectionManager() {
        return inspectionManager;
    }

    public void setInspectionManager(InspectionManager inspectionManager) {
        this.inspectionManager = inspectionManager;
    }

    public void addListener(BackupLoaderListener listener) {
        listeners.add(listener);
    }

    public void removeListener(BackupLoaderListener listener) {
        listeners.remove(listener);
    }

    public BackupLoaderListener[] getListeners() {
        int size = listeners.size();
        return listeners.toArray(new BackupLoaderListener[size]);
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

    public RowSetMapper getRowSetMapper() {
        return rowSetMapper;
    }

    public void setRowSetMapper(RowSetMapper rowSetMapper) {
        this.rowSetMapper = rowSetMapper;
    }

    public Collection<ScriptType> getScriptTypes() {
        return scriptTypes;
    }

    public void setScriptTypes(Collection<ScriptType> scriptTypes) {
        this.scriptTypes = scriptTypes;
    }

    public ConnectionSpec getTargetSpec() {
        return targetSpec;
    }

    public void setTargetSpec(ConnectionSpec targetSpec) {
        this.targetSpec = targetSpec;
    }

    public SessionFactory getTargetSessionFactory() {
        return targetSessionFactory;
    }

    public void setTargetSessionFactory(SessionFactory targetSessionFactory) {
        this.targetSessionFactory = targetSessionFactory;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public TranslationConfig getTranslationConfig() {
        return translationConfig;
    }

    public void setTranslationConfig(TranslationConfig translationConfig) {
        this.translationConfig = translationConfig;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public ScriptExporter getScriptExporter() {
        return scriptExporter;
    }

    public void setScriptExporter(ScriptExporter scriptExporter) {
        this.scriptExporter = scriptExporter;
    }

    public ValueFormatRegistryResolver getValueFormatRegistryResolver() {
        return valueFormatRegistryResolver;
    }

    public void setValueFormatRegistryResolver(ValueFormatRegistryResolver valueFormatRegistryResolver) {
        this.valueFormatRegistryResolver = valueFormatRegistryResolver;
    }
}
