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
package com.nuodb.migrator.backup.writer;

import com.nuodb.migrator.backup.Backup;
import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.spec.MigrationMode;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

import static com.nuodb.migrator.spec.MigrationMode.DATA;
import static com.nuodb.migrator.spec.MigrationMode.SCHEMA;
import static com.nuodb.migrator.utils.Collections.contains;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class SimpleBackupWriterContext implements BackupWriterContext {

    protected final transient Logger logger = getLogger(getClass());

    private Backup backup;
    private BackupOps backupOps;
    private Map backupOpsContext;
    private Database database;
    private ExecutorService executorService;
    private String format;
    private Map<String, Object> formatAttributes;
    private FormatFactory formatFactory;
    private Collection<MigrationMode> migrationModes;
    private Session sourceSession;
    private SessionFactory sourceSessionFactory;
    private TimeZone timeZone;
    private int threads;
    private ValueFormatRegistry valueFormatRegistry;
    private Collection<WriteQuery> writeQueries;

    @Override
    public boolean isWriteData() {
        return contains(getMigrationModes(), DATA);
    }

    @Override
    public boolean isWriteSchema() {
        return contains(getMigrationModes(), SCHEMA);
    }

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
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public Map<String, Object> getFormatAttributes() {
        return formatAttributes;
    }

    @Override
    public void setFormatAttributes(Map<String, Object> formatAttributes) {
        this.formatAttributes = formatAttributes;
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
    public Collection<MigrationMode> getMigrationModes() {
        return migrationModes;
    }

    public void setMigrationModes(Collection<MigrationMode> migrationModes) {
        this.migrationModes = migrationModes;
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
    public TimeZone getTimeZone() {
        return timeZone;
    }

    @Override
    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public int getThreads() {
        return threads;
    }

    @Override
    public void setThreads(int threads) {
        this.threads = threads;
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
    public Collection<WriteQuery> getWriteQueries() {
        return writeQueries;
    }

    @Override
    public void setWriteQueries(Collection<WriteQuery> writeQueries) {
        this.writeQueries = writeQueries;
    }
}
