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
package com.nuodb.migrator.dump;

import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.writer.BackupWriter;
import com.nuodb.migrator.backup.writer.BackupWriterListener;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.job.HasServicesJobBase;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.DumpJobSpec;
import com.nuodb.migrator.spec.MetaDataSpec;
import com.nuodb.migrator.spec.MigrationMode;
import com.nuodb.migrator.spec.QuerySpec;
import com.nuodb.migrator.spec.ResourceSpec;

import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import static com.nuodb.migrator.backup.writer.BackupWriter.THREADS;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newSessionTimeZoneSetter;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newTransactionIsolationSetter;
import static java.sql.Connection.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({ "all" })
public class DumpJob extends HasServicesJobBase<DumpJobSpec> {

    private BackupWriter backupWriter;

    public DumpJob() {
    }

    public DumpJob(DumpJobSpec jobSpec) {
        super(jobSpec);
    }

    @Override
    protected void init() throws Exception {
        super.init();

        BackupWriter backupWriter = new BackupWriter();
        for (BackupWriterListener listener : getListeners()) {
            backupWriter.addListener(listener);
        }
        backupWriter.setFormat(getFormat());
        backupWriter.setFormatAttributes(getFormatAttributes());
        backupWriter.setFormatFactory(createFormatFactory());
        backupWriter.setInspectionManager(createInspectionManager());
        backupWriter.setMetaDataSpec(getMetaDataSpec());
        backupWriter.setMigrationModes(getMigrationModes());
        backupWriter.setQueryLimit(getQueryLimit());
        backupWriter.setQuerySpecs(getQuerySpecs());
        backupWriter.setSourceSpec(getSourceSpec());
        backupWriter.setSourceSessionFactory(createSourceSessionFactory());
        backupWriter.setTimeZone(getTimeZone());
        backupWriter.setThreads(getThreads() != null ? getThreads() : THREADS);
        backupWriter.setValueFormatRegistryResolver(createValueFormatRegistryResolver());
        setBackupWriter(backupWriter);
    }

    protected SessionFactory createSourceSessionFactory() {
        SessionFactory sessionFactory = newSessionFactory(
                createConnectionProviderFactory().createConnectionProvider(getSourceSpec()), createDialectResolver());
        if (getSourceSpec().getTransactionIsolation() == null) {
            sessionFactory.addSessionObserver(newTransactionIsolationSetter(
                    new int[] { TRANSACTION_SERIALIZABLE, TRANSACTION_REPEATABLE_READ, TRANSACTION_READ_COMMITTED }));
        }
        sessionFactory.addSessionObserver(newSessionTimeZoneSetter(getTimeZone()));
        return sessionFactory;
    }

    @Override
    public void execute() throws Exception {
        try {
            BackupWriter backupWriter = getBackupWriter();
            backupWriter.write(getPath());
        } catch (MigratorException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new DumpException(exception);
        }
    }

    @Override
    public void close() throws Exception {
    }

    public BackupWriter getBackupWriter() {
        return backupWriter;
    }

    public void setBackupWriter(BackupWriter backupWriter) {
        this.backupWriter = backupWriter;
    }

    protected Collection<BackupWriterListener> getListeners() {
        return getJobSpec().getListeners();
    }

    protected String getFormat() {
        return getOutputSpec().getType();
    }

    protected Map<String, Object> getFormatAttributes() {
        return getOutputSpec().getAttributes();
    }

    protected Collection<MigrationMode> getMigrationModes() {
        return getJobSpec().getMigrationModes();
    }

    protected MetaDataSpec getMetaDataSpec() {
        return getJobSpec().getMetaDataSpec();
    }

    protected ResourceSpec getOutputSpec() {
        return getJobSpec().getOutputSpec();
    }

    protected String getPath() {
        return getOutputSpec().getPath();
    }

    public QueryLimit getQueryLimit() {
        return getJobSpec().getQueryLimit();
    }

    protected Collection<QuerySpec> getQuerySpecs() {
        return getJobSpec().getQuerySpecs();
    }

    protected ConnectionSpec getSourceSpec() {
        return getJobSpec().getSourceSpec();
    }

    protected TimeZone getTimeZone() {
        return getJobSpec().getTimeZone();
    }

    protected Integer getThreads() {
        return getJobSpec().getThreads();
    }
}
