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

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.format.value.Row;
import com.nuodb.migrator.jdbc.session.SimpleWorkManager;
import com.nuodb.migrator.jdbc.session.Work;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Multimaps.newSetMultimap;
import static com.google.common.collect.Multimaps.synchronizedSetMultimap;
import static com.google.common.collect.Sets.newTreeSet;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class SimpleBackupWriterManager extends SimpleWorkManager<BackupWriterListener> implements BackupWriterManager {

    private BackupWriterSync backupWriterSync;
    private BackupWriterContext backupWriterContext;
    private Multimap<WriteQuery, WriteQueryWork> writeQueries;

    public SimpleBackupWriterManager() {
        this.writeQueries = synchronizedSetMultimap(newSetMultimap(
                Maps.<WriteQuery, Collection<WriteQueryWork>>newHashMap(), new Supplier<Set<WriteQueryWork>>() {
                    @Override
                    public Set<WriteQueryWork> get() {
                        return newTreeSet(new Comparator<WriteQueryWork>() {
                            @Override
                            public int compare(WriteQueryWork w1, WriteQueryWork w2) {
                                return Ints.compare(w1.getQuerySplit().getSplitIndex(),
                                        w2.getQuerySplit().getSplitIndex());
                            }
                        });
                    }
                }));
    }

    @Override
    public boolean canExecute(Work work) {
        return getFailures().isEmpty();
    }

    @Override
    public void writeStart(Work work, WriteQuery writeQuery) {
        writeQueries.put(writeQuery, (WriteQueryWork) work);
        if (hasListeners()) {
            onWriteStart(new WriteChunkEvent(work, writeQuery));
        }
    }

    @Override
    public void writeStart(Work work, WriteQuery writeQuery, Chunk chunk) {
        if (hasListeners()) {
            onWriteStart(new WriteChunkEvent(work, writeQuery, chunk));
        }
    }

    protected void onWriteStart(WriteChunkEvent event) {
        for (BackupWriterListener listener : getListeners()) {
            listener.onWriteStart(event);
        }
    }

    @Override
    public void writeRow(Work work, WriteQuery writeQuery, Row row) {
        if (hasListeners()) {
            onWriteRow(new WriteRowEvent(work, writeQuery, row));
        }
    }

    protected void onWriteRow(WriteRowEvent event) {
        for (BackupWriterListener listener : getListeners()) {
            listener.onWriteRow(event);
        }
    }

    @Override
    public void writeEnd(Work work, WriteQuery writeQuery, Chunk chunk) {
        if (hasListeners()) {
            onWriteEnd(new WriteChunkEvent(work, writeQuery, chunk));
        }
    }

    @Override
    public void writeEnd(Work work, WriteQuery writeQuery) {
        RowSet rowSet = writeQuery.getRowSet();
        synchronized (rowSet) {
            writeQueries.put(writeQuery, (WriteQueryWork) work);
            Collection<Chunk> chunks = newArrayList();
            for (WriteQueryWork writeQueryWork : writeQueries.get(writeQuery)) {
                chunks.addAll(writeQueryWork.getChunks());
            }
            rowSet.setChunks(chunks);
        }
        if (hasListeners()) {
            onWriteEnd(new WriteChunkEvent(work, writeQuery));
        }
    }

    protected void onWriteEnd(WriteChunkEvent event) {
        for (BackupWriterListener listener : getListeners()) {
            listener.onWriteEnd(event);
        }
    }

    @Override
    protected void failure(Work work, Throwable failure) {
        try {
            super.failure(work, failure);
        } finally {
            writeFailed();
        }
    }

    @Override
    public boolean isWriteData() {
        return backupWriterContext.isWriteData();
    }

    @Override
    public boolean isWriteSchema() {
        return backupWriterContext.isWriteSchema();
    }

    @Override
    public void writeFailed() {
        backupWriterSync.writeFailed();
    }

    @Override
    public void writeDataDone() {
        backupWriterSync.writeDataDone();
    }

    @Override
    public void writeSchemaDone() {
        backupWriterSync.writeSchemaDone();
    }

    @Override
    public BackupWriterContext getBackupWriterContext() {
        return backupWriterContext;
    }

    @Override
    public void setBackupWriterContext(BackupWriterContext backupWriterContext) {
        this.backupWriterContext = backupWriterContext;
        this.backupWriterSync = new BackupWriterSync(backupWriterContext.isWriteData(),
                backupWriterContext.isWriteSchema());
    }

    @Override
    public void close() throws Exception {
        if (backupWriterSync != null) {
            backupWriterSync.await();
        }
        if (backupWriterContext != null) {
            ExecutorService executorService = backupWriterContext.getExecutorService();
            executorService.shutdown();
            try {
                executorService.awaitTermination(MAX_VALUE, SECONDS);
            } catch (InterruptedException exception) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Executor termination interrupted", exception);
                }
            }
            closeQuietly(backupWriterContext.getSourceSession());
        }
        super.close();
    }
}