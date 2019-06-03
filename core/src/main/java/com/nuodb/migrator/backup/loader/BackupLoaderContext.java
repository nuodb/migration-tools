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
package com.nuodb.migrator.backup.loader;

import com.nuodb.migrator.backup.Backup;
import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.jdbc.commit.CommitStrategy;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptExporter;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.MigrationMode;

import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

/**
 * @author Sergey Bushik
 */
public interface BackupLoaderContext {

    boolean isLoadData();

    boolean isLoadSchema();

    Backup getBackup();

    void setBackup(Backup backup);

    BackupOps getBackupOps();

    void setBackupOps(BackupOps backupOps);

    Map getBackupOpsContext();

    void setBackupOpsContext(Map backupOpsContext);

    CommitStrategy getCommitStrategy();

    void setCommitStrategy(CommitStrategy commitStrategy);

    Database getDatabase();

    void setDatabase(Database database);

    ExecutorService getExecutorService();

    void setExecutorService(ExecutorService executorService);

    Map<String, Object> getFormatAttributes();

    void setFormatAttributes(Map<String, Object> formatAttributes);

    FormatFactory getFormatFactory();

    void setFormatFactory(FormatFactory formatFactory);

    Parallelizer getParallelizer();

    void setParallelizer(Parallelizer parallelizer);

    InsertTypeFactory getInsertTypeFactory();

    void setInsertTypeFactory(InsertTypeFactory insertTypeFactory);

    LoadConstraints getLoadConstraints();

    void setLoadConstraints(LoadConstraints loadConstraints);

    LoadTables getLoadTables();

    void setLoadTables(LoadTables loadTables);

    Collection<MigrationMode> getMigrationModes();

    void setMigrationModes(Collection<MigrationMode> migrationModes);

    RowSetMapper getRowSetMapper();

    void setRowSetMapper(RowSetMapper rowSetMapper);

    ConnectionSpec getSourceSpec();

    void setSourceSpec(ConnectionSpec sourceSpec);

    Session getSourceSession();

    void setSourceSession(Session sourceSession);

    SessionFactory getSourceSessionFactory();

    void setSourceSessionFactory(SessionFactory sourceSessionFactory);

    Collection<Table> getSourceTables();

    void setSourceTables(Collection<Table> tables);

    ConnectionSpec getTargetSpec();

    void setTargetSpec(ConnectionSpec targetSpec);

    Session getTargetSession();

    void setTargetSession(Session targetSession);

    SessionFactory getTargetSessionFactory();

    void setTargetSessionFactory(SessionFactory targetSessionFactory);

    ScriptExporter getScriptExporter();

    void setScriptExporter(ScriptExporter scriptExporter);

    ScriptGeneratorManager getScriptGeneratorManager();

    void setScriptGeneratorManager(ScriptGeneratorManager scriptGeneratorManager);

    TimeZone getTimeZone();

    void setTimeZone(TimeZone timeZone);

    ValueFormatRegistry getValueFormatRegistry();

    void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry);

}
