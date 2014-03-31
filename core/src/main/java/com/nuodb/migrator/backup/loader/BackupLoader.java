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

import com.nuodb.migrator.backup.Backup;
import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;
import com.nuodb.migrator.jdbc.dialect.ImplicitDefaultsTranslator;
import com.nuodb.migrator.jdbc.dialect.TranslationManager;
import com.nuodb.migrator.jdbc.dialect.Translator;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.NamingStrategy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.jdbc.type.JdbcTypeNameMap;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.JdbcTypeSpec;
import com.nuodb.migrator.utils.PrioritySet;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.NUODB;
import static com.nuodb.migrator.jdbc.metadata.generator.HasTablesScriptGenerator.GROUP_SCRIPTS_BY;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;
import static java.lang.Runtime.getRuntime;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class BackupLoader {

    public static final boolean LOAD_SCHEMA = true;
    public static final boolean LOAD_DATA = true;
    public static final int THREADS = getRuntime().availableProcessors();

    private boolean loadData = LOAD_DATA;
    private boolean loadSchema = LOAD_SCHEMA;
    private int threads = THREADS;
    private ConnectionSpec targetSpec;
    private Session targetSession;
    private SessionFactory targetSessionFactory;
    private DialectResolver dialectResolver;
    private BackupOps backupOps;
    private FormatFactory formatFactory;
    private Map<String, Object> formatAttributes = newHashMap();
    private ValueFormatRegistry valueFormatRegistry;
    private boolean useExplicitDefaults;
    private PrioritySet<NamingStrategy> namingStrategies;
    private GroupScriptsBy groupScriptsBy;
    private Collection<JdbcTypeSpec> jdbcTypeSpecs;
    private IdentifierQuoting identifierQuoting;
    private IdentifierNormalizer identifierNormalizer;
    private Collection<MetaDataType> objectTypes;
    private String[] tableTypes;
    private Collection<ScriptType> scriptTypes;
    private RowSetMapper rowSetMapper = new SimpleRowSetMapper();

    public Backup load(Map context) throws Exception {
        Backup backup = getBackupOps().read(context);
        load(backup, context);
        return backup;
    }

    public void load(Backup backup, Map context) throws Exception {
        load(createBackupLoaderContext(backup, context));
    }

    public void load(BackupLoaderContext backupLoaderContext) {
        // TODO: implement multi-threaded load
    }

    protected BackupLoaderContext createBackupLoaderContext(Backup backup, Map context) throws Exception {
        BackupLoaderContext backupLoaderContext = new SimpleBackupLoaderContext();
        backupLoaderContext.setBackup(backup);
        backupLoaderContext.setBackupOps(getBackupOps());
        backupLoaderContext.setBackupOpsContext(context);
        backupLoaderContext.setFormatFactory(getFormatFactory());
        backupLoaderContext.setTargetSpec(getTargetSpec());
        backupLoaderContext.setTargetSession(getTargetSession());
        backupLoaderContext.setTargetSessionFactory(getTargetSessionFactory());
        backupLoaderContext.setRowSetMapper(getRowSetMapper());

        Database database = backup.getDatabase();
        backupLoaderContext.setSourceSpec(database.getConnectionSpec());
        SessionFactory sourceSessionFactory = createSourceSessionFactory(database);
        backupLoaderContext.setSourceSession(sourceSessionFactory.openSession());
        backupLoaderContext.setSourceSessionFactory(sourceSessionFactory);
        backupLoaderContext.setScriptGeneratorManager(
                createScriptGeneratorManager(backupLoaderContext));
        return backupLoaderContext;
    }

    protected SessionFactory createSourceSessionFactory(Database database) {
        return newSessionFactory(database.getDialect(), database.getConnectionSpec());
    }

    protected ScriptGeneratorManager createScriptGeneratorManager(
            BackupLoaderContext backupLoaderContext) throws SQLException {
        ScriptGeneratorManager scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.getAttributes().put(GROUP_SCRIPTS_BY, getGroupScriptsBy());
        scriptGeneratorManager.setObjectTypes(getObjectTypes());
        scriptGeneratorManager.setScriptTypes(getScriptTypes());

        ConnectionSpec sourceSpec = backupLoaderContext.getSourceSpec();
        scriptGeneratorManager.setSourceCatalog(sourceSpec != null ? sourceSpec.getCatalog() : null);
        scriptGeneratorManager.setSourceSchema(sourceSpec != null ? sourceSpec.getSchema() : null);
        scriptGeneratorManager.setSourceSession(backupLoaderContext.getSourceSession());

        ConnectionSpec targetSpec = getTargetSpec();
        if (targetSpec != null) {
            scriptGeneratorManager.setTargetCatalog(targetSpec.getCatalog());
            scriptGeneratorManager.setTargetSchema(targetSpec.getSchema());
        }

        DialectResolver dialectResolver = getDialectResolver();
        Dialect dialect = getTargetSession() != null ? dialectResolver.resolve(
                getTargetSession().getConnection()) : dialectResolver.resolve(NUODB);
        TranslationManager translationManager = dialect.getTranslationManager();
        PrioritySet<Translator> translators = translationManager.getTranslators();
        for (Translator translator : translators) {
            if (translator instanceof ImplicitDefaultsTranslator) {
                ((ImplicitDefaultsTranslator) translator).setUseExplicitDefaults(isUseExplicitDefaults());
            }
        }
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
        if (namingStrategies != null) {
            for (PrioritySet.Entry<NamingStrategy> entry : namingStrategies.entries()) {
                scriptGeneratorManager.addNamingStrategy(entry.getValue(), entry.getPriority());
            }
        }
        scriptGeneratorManager.setTargetDialect(dialect);
        return scriptGeneratorManager;
    }

    public boolean isLoadData() {
        return loadData;
    }

    public void setLoadData(boolean loadData) {
        this.loadData = loadData;
    }

    public boolean isLoadSchema() {
        return loadSchema;
    }

    public void setLoadSchema(boolean loadSchema) {
        this.loadSchema = loadSchema;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public ConnectionSpec getTargetSpec() {
        return targetSpec;
    }

    public void setTargetSpec(ConnectionSpec targetSpec) {
        this.targetSpec = targetSpec;
    }

    public Session getTargetSession() {
        return targetSession;
    }

    public void setTargetSession(Session targetSession) {
        this.targetSession = targetSession;
    }

    public SessionFactory getTargetSessionFactory() {
        return targetSessionFactory;
    }

    public void setTargetSessionFactory(SessionFactory targetSessionFactory) {
        this.targetSessionFactory = targetSessionFactory;
    }

    public BackupOps getBackupOps() {
        return backupOps;
    }

    public void setBackupOps(BackupOps backupOps) {
        this.backupOps = backupOps;
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

    public ValueFormatRegistry getValueFormatRegistry() {
        return valueFormatRegistry;
    }

    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        this.valueFormatRegistry = valueFormatRegistry;
    }

    public boolean isUseExplicitDefaults() {
        return useExplicitDefaults;
    }

    public void setUseExplicitDefaults(boolean useExplicitDefaults) {
        this.useExplicitDefaults = useExplicitDefaults;
    }

    public PrioritySet<NamingStrategy> getNamingStrategies() {
        return namingStrategies;
    }

    public void setNamingStrategies(PrioritySet<NamingStrategy> namingStrategies) {
        this.namingStrategies = namingStrategies;
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

    public Collection<MetaDataType> getObjectTypes() {
        return objectTypes;
    }

    public void setObjectTypes(Collection<MetaDataType> objectTypes) {
        this.objectTypes = objectTypes;
    }

    public String[] getTableTypes() {
        return tableTypes;
    }

    public void setTableTypes(String[] tableTypes) {
        this.tableTypes = tableTypes;
    }

    public Collection<ScriptType> getScriptTypes() {
        return scriptTypes;
    }

    public void setScriptTypes(Collection<ScriptType> scriptTypes) {
        this.scriptTypes = scriptTypes;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    public RowSetMapper getRowSetMapper() {
        return rowSetMapper;
    }

    public void setRowSetMapper(RowSetMapper rowSetMapper) {
        this.rowSetMapper = rowSetMapper;
    }
}
