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
import com.nuodb.migrator.jdbc.JdbcUtils;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.MigrationMode;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.nuodb.migrator.spec.MigrationMode.DATA;
import static com.nuodb.migrator.spec.MigrationMode.SCHEMA;
import static com.nuodb.migrator.utils.Collections.contains;
import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class SimpleBackupLoaderContext implements BackupLoaderContext {

    protected final transient Logger logger = getLogger(getClass());

    private Backup backup;
    private BackupOps backupOps;
    private Map backupOpsContext;
    private Database database;
    private Executor executor;
    private FormatFactory formatFactory;
    private ConnectionSpec sourceSpec;
    private Collection<MigrationMode> migrationModes;
    private Session sourceSession;
    private SessionFactory sourceSessionFactory;
    private ConnectionSpec targetSpec;
    private Session targetSession;
    private SessionFactory targetSessionFactory;
    private ScriptExporter scriptExporter;
    private ScriptGeneratorManager scriptGeneratorManager;
    private ValueFormatRegistry valueFormatRegistry;
    private RowSetMapper rowSetMapper = new SimpleRowSetMapper();

    @Override
    public Backup getBackup() {
        return backup;
    }

    @Override
    public void setBackup(Backup backup) {
        this.backup = backup;
    }

    @Override
    public BackupOps getBackupOps() {
        return backupOps;
    }

    @Override
    public void setBackupOps(BackupOps backupOps) {
        this.backupOps = backupOps;
    }

    @Override
    public Map getBackupOpsContext() {
        return backupOpsContext;
    }

    @Override
    public void setBackupOpsContext(Map backupOpsContext) {
        this.backupOpsContext = backupOpsContext;
    }

    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public void setDatabase(Database database) {
        this.database = database;
    }

    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    @Override
    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    @Override
    public boolean isLoadData() {
        return contains(migrationModes, DATA);
    }

    @Override
    public boolean isLoadSchema() {
        return contains(migrationModes, SCHEMA);
    }

    @Override
    public Collection<MigrationMode> getMigrationModes() {
        return migrationModes;
    }

    public void setMigrationModes(Collection<MigrationMode> migrationModes) {
        this.migrationModes = migrationModes;
    }

    @Override
    public ConnectionSpec getSourceSpec() {
        return sourceSpec;
    }

    @Override
    public void setSourceSpec(ConnectionSpec sourceSpec) {
        this.sourceSpec = sourceSpec;
    }

    @Override
    public Session getSourceSession() {
        return sourceSession;
    }

    @Override
    public void setSourceSession(Session sourceSession) {
        this.sourceSession = sourceSession;
    }

    @Override
    public SessionFactory getSourceSessionFactory() {
        return sourceSessionFactory;
    }

    @Override
    public void setSourceSessionFactory(SessionFactory sourceSessionFactory) {
        this.sourceSessionFactory = sourceSessionFactory;
    }

    @Override
    public ConnectionSpec getTargetSpec() {
        return targetSpec;
    }

    @Override
    public void setTargetSpec(ConnectionSpec targetSpec) {
        this.targetSpec = targetSpec;
    }

    @Override
    public Session getTargetSession() {
        return targetSession;
    }

    @Override
    public void setTargetSession(Session targetSession) {
        this.targetSession = targetSession;
    }

    @Override
    public SessionFactory getTargetSessionFactory() {
        return targetSessionFactory;
    }

    @Override
    public void setTargetSessionFactory(SessionFactory targetSessionFactory) {
        this.targetSessionFactory = targetSessionFactory;
    }

    @Override
    public ScriptExporter getScriptExporter() {
        return scriptExporter;
    }

    @Override
    public void setScriptExporter(ScriptExporter scriptExporter) {
        this.scriptExporter = scriptExporter;
    }

    @Override
    public ScriptGeneratorManager getScriptGeneratorManager() {
        return scriptGeneratorManager;
    }

    @Override
    public void setScriptGeneratorManager(ScriptGeneratorManager scriptGeneratorManager) {
        this.scriptGeneratorManager = scriptGeneratorManager;
    }

    @Override
    public ValueFormatRegistry getValueFormatRegistry() {
        return valueFormatRegistry;
    }

    @Override
    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        this.valueFormatRegistry = valueFormatRegistry;
    }

    @Override
    public RowSetMapper getRowSetMapper() {
        return rowSetMapper;
    }

    @Override
    public void setRowSetMapper(RowSetMapper rowSetMapper) {
        this.rowSetMapper = rowSetMapper;
    }

    @Override
    public void close(boolean awaitTermination) {
        if (executor instanceof ExecutorService) {
            ExecutorService service = (ExecutorService) executor;
            service.shutdown();
            try {
                if (awaitTermination) {
                    service.awaitTermination(MAX_VALUE, SECONDS);
                }
            } catch (InterruptedException exception) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Executor termination interrupted", exception);
                }
            }
        }
        JdbcUtils.close(sourceSession);
        JdbcUtils.close(targetSession);
        JdbcUtils.close(scriptExporter);
    }
}
