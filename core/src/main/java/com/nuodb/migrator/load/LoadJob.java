/**
 * Copyright (c) 2015, NuoDB, Inc.
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

import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.loader.*;
import com.nuodb.migrator.jdbc.commit.CommitStrategy;
import com.nuodb.migrator.jdbc.dialect.TranslationConfig;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.jdbc.query.InsertType;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.job.ScriptGeneratorJobBase;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.LoadJobSpec;
import com.nuodb.migrator.spec.MigrationMode;
import com.nuodb.migrator.spec.ResourceSpec;

import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import static com.nuodb.migrator.backup.loader.BackupLoader.THREADS;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newSessionTimeZoneSetter;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("ConstantConditions")
public class LoadJob extends ScriptGeneratorJobBase<LoadJobSpec> {

    private BackupLoader backupLoader;

    public LoadJob(LoadJobSpec jobSpec) {
        super(jobSpec);
    }

    @Override
    protected void init() throws Exception {
        super.init();

        BackupLoader backupLoader = new BackupLoader();
        for (BackupLoaderListener listener : getListeners()) {
            backupLoader.addListener(listener);
        }
        backupLoader.setCommitStrategy(getCommitStrategy());
        backupLoader.setDialectResolver(createDialectResolver());
        backupLoader.setFormatAttributes(getFormatAttributes());
        backupLoader.setFormatFactory(createFormatFactory());
        backupLoader.setJdbcTypeSpecs(getJdbcTypeSpecs());
        backupLoader.setGroupScriptsBy(getGroupScriptsBy());
        backupLoader.setIdentifierNormalizer(getIdentifierNormalizer());
        backupLoader.setIdentifierQuoting(getIdentifierQuoting());
        backupLoader.setInsertTypeFactory(createInsertTypeMapper());
        backupLoader.setInspectionManager(createInspectionManager());
        backupLoader.setMetaDataSpec(getMetaDataSpec());
        backupLoader.setMigrationModes(getMigrationModes());
        backupLoader.setNamingStrategies(getNamingStrategies());
        backupLoader.setParallelizer(getParallelizer());
        backupLoader.setScriptTypes(getScriptTypes());
        backupLoader.setMetaDataFilterManager(getMetaDataFilterManager());
        backupLoader.setTargetSpec(getTargetSpec());
        backupLoader.setTargetSessionFactory(createTargetSessionFactory());
        backupLoader.setTimeZone(getTimeZone());
        backupLoader.setThreads(getThreads() != null ? getThreads() : THREADS);
        backupLoader.setTranslationConfig(getTranslationConfig());
        backupLoader.setValueFormatRegistryResolver(createValueFormatRegistryResolver());
        setBackupLoader(backupLoader);
    }

    protected InsertTypeFactory createInsertTypeMapper() {
        return new SimpleInsertTypeFactory(getInsertType(), getTableInsertTypes());
    }

    protected SessionFactory createTargetSessionFactory() {
        SessionFactory targetSessionFactory = newSessionFactory(
                createConnectionProviderFactory().createConnectionProvider(getTargetSpec()), createDialectResolver());
        targetSessionFactory.addSessionObserver(newSessionTimeZoneSetter(getTimeZone()));
        return targetSessionFactory;
    }

    @Override
    public void execute() throws Exception {
        try {
            BackupLoader backupLoader = getBackupLoader();
            backupLoader.load(getPath());
        } catch (MigratorException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new LoadException(exception);
        }
    }

    @Override
    public void close() throws Exception {
    }

    protected BackupLoader getBackupLoader() {
        return backupLoader;
    }

    protected void setBackupLoader(BackupLoader backupLoader) {
        this.backupLoader = backupLoader;
    }

    protected Collection<BackupLoaderListener> getListeners() {
        return getJobSpec().getListeners();
    }

    protected CommitStrategy getCommitStrategy() {
        return getJobSpec().getCommitStrategy();
    }

    protected Map<String, Object> getFormatAttributes() {
        return getInputSpec().getAttributes();
    }

    protected String getPath() {
        return getInputSpec().getPath();
    }

    protected Collection<MigrationMode> getMigrationModes() {
        return getJobSpec().getMigrationModes();
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

    protected Parallelizer getParallelizer() {
        return getJobSpec().getParallelizer();
    }

    protected MetaDataFilterManager getMetaDataFilterManager() {
        return getJobSpec().getMetaDataFilterManager();
    }

    protected ConnectionSpec getTargetSpec() {
        return getJobSpec().getTargetSpec();
    }

    protected TimeZone getTimeZone() {
        return getJobSpec().getTimeZone();
    }

    protected TranslationConfig getTranslationConfig() {
        return getJobSpec().getTranslationConfig();
    }

    protected Integer getThreads() {
        return getJobSpec().getThreads();
    }
}