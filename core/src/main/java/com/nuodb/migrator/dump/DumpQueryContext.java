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

import com.nuodb.migrator.backup.BackupManager;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.csv.CsvAttributes;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.session.SessionFactory;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import static java.lang.Runtime.getRuntime;

/**
 * @author Sergey Bushik
 */
public interface DumpQueryContext {

    final String FORMAT = CsvAttributes.FORMAT;

    final int THREADS = getRuntime().availableProcessors();

    int getThreads();

    void setThreads(int threads);

    Database getDatabase();

    void setDatabase(Database database);

    Executor getExecutor();

    void setExecutor(Executor executor);

    TimeZone getTimeZone();

    void setTimeZone(TimeZone timeZone);

    Session getSession();

    void setSession(Session session);

    SessionFactory getSessionFactory();

    void setSessionFactory(SessionFactory sessionFactory);

    BackupManager getBackupManager();

    void setBackupManager(BackupManager backupManager);

    String getFormat();

    void setFormat(String format);

    Map<String, Object> getFormatAttributes();

    void setFormatAttributes(Map<String, Object> formatAttributes);

    FormatFactory getFormatFactory();

    void setFormatFactory(FormatFactory formatFactory);

    ValueFormatRegistry getValueFormatRegistry();

    void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry);
}
