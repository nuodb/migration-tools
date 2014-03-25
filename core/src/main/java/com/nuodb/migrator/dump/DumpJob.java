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
package com.nuodb.migrator.dump;

import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.backup.writer.BackupWriter;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.jdbc.metadata.inspector.TableInspectionScope;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.job.HasServicesJobBase;
import com.nuodb.migrator.spec.*;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.backup.XmlMetaDataHandlerBase.META_DATA_SPEC;
import static com.nuodb.migrator.backup.writer.BackupWriter.THREADS;
import static com.nuodb.migrator.context.ContextUtils.createService;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.DATABASE;
import static com.nuodb.migrator.jdbc.session.SessionFactories.newSessionFactory;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newSessionTimeZoneSetter;
import static com.nuodb.migrator.jdbc.session.SessionObservers.newTransactionIsolationSetter;
import static com.nuodb.migrator.spec.MigrationMode.DATA;
import static com.nuodb.migrator.spec.MigrationMode.SCHEMA;
import static com.nuodb.migrator.utils.Collections.contains;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.sql.Connection.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"unchecked", "ToArrayCallWithZeroLengthArrayArgument"})
public class DumpJob extends HasServicesJobBase<DumpJobSpec> {

    private BackupWriter backupWriter;
    private Session sourceSession;

    public DumpJob() {
    }

    public DumpJob(DumpJobSpec jobSpec) {
        super(jobSpec);
    }

    @Override
    protected void init() throws Exception {
        super.init();

        SessionFactory sessionFactory = createSourceSessionFactory();
        Session session = sessionFactory.openSession();
        setSourceSession(session);

        BackupWriter backupWriter = new BackupWriter();
        backupWriter.setBackupOps(createBackupOps());
        backupWriter.setQueryLimit(getQueryLimit());
        backupWriter.setThreads(getThreads() != null ? getThreads() : THREADS);
        backupWriter.setTimeZone(getTimeZone());
        backupWriter.setFormat(getFormat());
        backupWriter.setFormatAttributes(getFormatAttributes());
        backupWriter.setFormatFactory(createFormatFactory());
        backupWriter.setSourceSession(session);
        backupWriter.setSourceSessionFactory(sessionFactory);
        backupWriter.setValueFormatRegistry(createValueFormatRegistryResolver().
                resolve(session.getConnection()));
        backupWriter.setWriteData(contains(getMigrationModes(), DATA));
        backupWriter.setWriteSchema(contains(getMigrationModes(), SCHEMA));
        setBackupWriter(backupWriter);
    }

    protected BackupOps createBackupOps() {
        BackupOps backupOps = createService(getBackupOps(), BackupOps.class);
        backupOps.setPath(getPath());
        return backupOps;
    }

    protected SessionFactory createSourceSessionFactory() {
        SessionFactory sessionFactory = newSessionFactory(
                createConnectionProviderFactory().
                        createConnectionProvider(getSourceSpec()), createDialectResolver());
        if (getSourceSpec().getTransactionIsolation() == null) {
            sessionFactory.addSessionObserver(newTransactionIsolationSetter(new int[]{
                    TRANSACTION_SERIALIZABLE,
                    TRANSACTION_REPEATABLE_READ,
                    TRANSACTION_READ_COMMITTED
            }));
        }
        sessionFactory.addSessionObserver(newSessionTimeZoneSetter(getTimeZone()));
        return sessionFactory;
    }

    @Override
    public void execute() throws Exception {
        try {
            Database database = inspect();
            BackupWriter backupWriter = getBackupWriter();
            if (backupWriter.isWriteData()) {
                Collection<TableSpec> tableSpecs = getTableSpecs();
                if (isEmpty(tableSpecs)) {
                    backupWriter.addTables(database, getTableTypes());
                } else {
                    backupWriter.addTables(tableSpecs);
                }
                for (QuerySpec querySpec : getQuerySpecs()) {
                    backupWriter.addQuery(querySpec.getQuery());
                }
            }
            backupWriter.setDatabase(backupWriter.isWriteSchema() ? database : null);
            Map context = newHashMap();
            context.put(META_DATA_SPEC, getMetaDataSpec());
            backupWriter.write(context);
        } catch (MigratorException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new DumpException(exception);
        }
    }

    @Override
    public void release() throws Exception {
        close(sourceSession);
    }

    protected Database inspect() throws SQLException {
        InspectionScope inspectionScope = new TableInspectionScope(
                getSourceSpec().getCatalog(), getSourceSpec().getSchema(), getTableTypes());
        return createInspectionManager().inspect(getSourceSession().getConnection(), inspectionScope,
                getObjectTypes().toArray(new MetaDataType[0])).getObject(DATABASE);
    }

    public BackupWriter getBackupWriter() {
        return backupWriter;
    }

    public void setBackupWriter(BackupWriter backupWriter) {
        this.backupWriter = backupWriter;
    }

    public Session getSourceSession() {
        return sourceSession;
    }

    public void setSourceSession(Session sourceSession) {
        this.sourceSession = sourceSession;
    }

    protected BackupOps getBackupOps() {
        return getJobSpec().getBackupOps();
    }

    protected String getFormat() {
        return getOutputSpec().getType();
    }

    protected Map<String, Object> getFormatAttributes() {
        return getOutputSpec().getAttributes();
    }

    protected String getPath() {
        return getOutputSpec().getPath();
    }

    protected Collection<MigrationMode> getMigrationModes() {
        return getJobSpec().getMigrationModes();
    }

    protected Integer getThreads() {
        return getJobSpec().getThreads();
    }

    protected ResourceSpec getOutputSpec() {
        return getJobSpec().getOutputSpec();
    }

    protected Collection<QuerySpec> getQuerySpecs() {
        return getJobSpec().getQuerySpecs();
    }

    protected MetaDataSpec getMetaDataSpec() {
        return getJobSpec().getMetaDataSpec();
    }

    protected Collection<TableSpec> getTableSpecs() {
        return getJobSpec().getTableSpecs();
    }

    protected Collection<MetaDataType> getObjectTypes() {
        return getJobSpec().getObjectTypes();
    }

    protected String[] getTableTypes() {
        return getJobSpec().getTableTypes();
    }

    protected TimeZone getTimeZone() {
        return getJobSpec().getTimeZone();
    }

    protected ConnectionSpec getSourceSpec() {
        return getJobSpec().getSourceSpec();
    }

    public QueryLimit getQueryLimit() {
        return getJobSpec().getQueryLimit();
    }
}
