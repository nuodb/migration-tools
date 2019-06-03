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

import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.format.value.Row;
import com.nuodb.migrator.jdbc.session.SimpleWorkManager;
import com.nuodb.migrator.jdbc.session.Work;

import java.util.concurrent.ExecutorService;

import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;
import static java.lang.Long.MAX_VALUE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Sergey Bushik
 */
public class SimpleBackupLoaderManager extends SimpleWorkManager<BackupLoaderListener> implements BackupLoaderManager {

    private BackupLoaderSync backupLoaderSync;
    private BackupLoaderContext backupLoaderContext;

    @Override
    public boolean canExecute(Work work) {
        return getFailures().isEmpty();
    }

    @Override
    public void beforeLoadRow(Work work, LoadTable loadTable, Row row) {
        Chunk chunk = row.getChunk();
        long number = row.getNumber();
        if (number == 0) {
            if (logger.isTraceEnabled()) {
                logger.trace(format("Loading %d rows from %s chunk", chunk.getRowCount(), chunk.getName()));
            }
            if (hasListeners()) {
                onStartChunk(new LoadChunkEvent(work, loadTable, row.getChunk()));
            }
        }
    }

    protected void onStartChunk(LoadChunkEvent loadChunkEvent) {
        for (BackupLoaderListener listener : getListeners()) {
            listener.onLoadStart(loadChunkEvent);
        }
    }

    @Override
    public void afterLoadRow(Work work, LoadTable loadTable, Row row) {
        Chunk chunk = row.getChunk();
        long number = row.getNumber();
        if (number == chunk.getRowSet().getRowCount()) {
            if (logger.isTraceEnabled()) {
                logger.trace(format("Rows from %s chunk loaded", chunk.getName()));
            }
            if (hasListeners()) {
                onEndChunk(new LoadChunkEvent(work, loadTable, row.getChunk()));
            }
        }
        onLoadRow(new LoadRowEvent(work, loadTable, row));
    }

    protected void onEndChunk(LoadChunkEvent loadChunkEvent) {
        for (BackupLoaderListener listener : getListeners()) {
            listener.onLoadEnd(loadChunkEvent);
        }
    }

    protected void onLoadRow(LoadRowEvent event) {
        for (BackupLoaderListener listener : getListeners()) {
            listener.onLoadRow(event);
        }
    }

    @Override
    public void loadFailed() {
        backupLoaderSync.loadFailed();
    }

    @Override
    public void loadDataDone() {
        backupLoaderSync.loadDataDone();
    }

    @Override
    public void loadConstraintsDone() {
        backupLoaderSync.loadConstraintsDone();
    }

    @Override
    public void loadSchemaDone() {
        backupLoaderSync.loadSchemaDone();
    }

    @Override
    protected void failure(Work work, Throwable failure) {
        try {
            super.failure(work, failure);
        } finally {
            loadFailed();
        }
    }

    @Override
    public boolean isLoadData() {
        return backupLoaderContext != null && backupLoaderContext.isLoadData();
    }

    @Override
    public boolean isLoadSchema() {
        return backupLoaderContext != null && backupLoaderContext.isLoadSchema();
    }

    @Override
    public BackupLoaderContext getBackupLoaderContext() {
        return backupLoaderContext;
    }

    @Override
    public void setBackupLoaderContext(BackupLoaderContext backupLoaderContext) {
        isNotNull(backupLoaderContext, "Backup loader context is required");
        this.backupLoaderContext = backupLoaderContext;
        this.backupLoaderSync = new BackupLoaderSync(backupLoaderContext.isLoadData(),
                backupLoaderContext.isLoadSchema());
    }

    @Override
    public void close() throws Exception {
        if (backupLoaderSync != null) {
            backupLoaderSync.await();
        }
        if (backupLoaderContext != null) {
            ExecutorService executorService = backupLoaderContext.getExecutorService();
            executorService.shutdown();
            try {
                executorService.awaitTermination(MAX_VALUE, SECONDS);
            } catch (InterruptedException exception) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Executor termination interrupted", exception);
                }
            }
            closeQuietly(backupLoaderContext.getSourceSession());
            closeQuietly(backupLoaderContext.getTargetSession());
            closeQuietly(backupLoaderContext.getScriptExporter());
        }
        super.close();
    }
}
