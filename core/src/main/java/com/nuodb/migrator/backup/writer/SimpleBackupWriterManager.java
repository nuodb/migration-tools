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
package com.nuodb.migrator.backup.writer;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.jdbc.session.SimpleWorkManager;
import com.nuodb.migrator.jdbc.session.Work;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.Executor;
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
public class SimpleBackupWriterManager extends SimpleWorkManager<BackupWriterListener>
        implements BackupWriterManager {

    private BackupWriterSync backupWriterSync;
    private BackupWriterContext backupWriterContext;
    private Long deltaRowCount;

    private Multimap<WriteRowSet, WriteRowSetWork> writeRowSets;

    public SimpleBackupWriterManager() {
        writeRowSets = synchronizedSetMultimap(newSetMultimap(
                Maps.<WriteRowSet, Collection<WriteRowSetWork>>newHashMap(),
                new Supplier<Set<WriteRowSetWork>>() {
                    @Override
                    public Set<WriteRowSetWork> get() {
                        return newTreeSet(new Comparator<WriteRowSetWork>() {
                            @Override
                            public int compare(WriteRowSetWork w1, WriteRowSetWork w2) {
                                return Ints.compare(w1.getQuerySplit().getSplitIndex(),
                                        w2.getQuerySplit().getSplitIndex());
                            }
                        });
                    }
                }));
    }

    @Override
    public boolean canWrite(Work work, WriteRowSet writeRowSet) {
        return getFailures().isEmpty();
    }

    @Override
    public void writeStart(Work work, WriteRowSet writeRowSet) {
        writeRowSets.put(writeRowSet, (WriteRowSetWork) work);
        if (hasListeners()) {
            onWriteStart(new WriteRowSetEvent(work, writeRowSet));
        }
    }

    @Override
    public void writeStart(Work work, WriteRowSet writeRowSet, Chunk chunk) {
        if (hasListeners()) {
            onWriteStart(new WriteRowSetEvent(work, writeRowSet, chunk));
        }
    }

    protected void onWriteStart(WriteRowSetEvent event) {
        for (BackupWriterListener listener : getListeners()) {
            listener.onWriteStart(event);
        }
    }

    @Override
    public void writeRow(Work work, WriteRowSet writeRowSet, Chunk chunk) {
        Long deltaRowCount = getDeltaRowCount();
        if (hasListeners() && (deltaRowCount != null && chunk.getRowCount() % deltaRowCount == 0)) {
            onWriteRow(new WriteRowSetEvent(work, writeRowSet, chunk));
        }
    }

    protected void onWriteRow(WriteRowSetEvent event) {
        for (BackupWriterListener listener : getListeners()) {
            listener.onWriteRow(event);
        }
    }

    @Override
    public void writeEnd(Work work, WriteRowSet writeRowSet, Chunk chunk) {
        if (hasListeners()) {
            onWriteEnd(new WriteRowSetEvent(work, writeRowSet, chunk));
        }
    }

    @Override
    public void writeEnd(Work work, WriteRowSet writeRowSet) {
        RowSet rowSet = writeRowSet.getRowSet();
        synchronized (rowSet) {
            writeRowSets.put(writeRowSet, (WriteRowSetWork) work);
            Collection<Chunk> chunks = newArrayList();
            for (WriteRowSetWork writeRowSetWork : writeRowSets.get(writeRowSet)) {
                chunks.addAll(writeRowSetWork.getChunks());
            }
            rowSet.setChunks(chunks);
        }
        if (hasListeners()) {
            onWriteEnd(new WriteRowSetEvent(work, writeRowSet));
        }
    }

    protected void onWriteEnd(WriteRowSetEvent event) {
        for (BackupWriterListener listener : getListeners()) {
            listener.onWriteEnd(event);
        }
    }

    @Override
    protected void failure(Work work, Throwable failure) {
        super.failure(work, failure);
        writeFailed();
    }

    @Override
    public boolean isWriteData() {
        BackupWriterContext backupWriterContext = getBackupWriterContext();
        return backupWriterContext != null && backupWriterContext.isWriteData();
    }

    @Override
    public boolean isWriteSchema() {
        BackupWriterContext backupWriterContext = getBackupWriterContext();
        return backupWriterContext != null && backupWriterContext.isWriteSchema();
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
    public Long getDeltaRowCount() {
        return deltaRowCount;
    }

    @Override
    public void setDeltaRowCount(Long deltaRowCount) {
        this.deltaRowCount = deltaRowCount;
    }


    @Override
    public void close() throws Exception {
        backupWriterSync.await();

        Executor executor = backupWriterContext.getExecutor();
        if (executor instanceof ExecutorService) {
            ExecutorService service = (ExecutorService) executor;
            service.shutdown();
            try {
                if (!backupWriterSync.isFailed()) {
                    service.awaitTermination(MAX_VALUE, SECONDS);
                }
            } catch (InterruptedException exception) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Executor termination interrupted", exception);
                }
            }
        }
        closeQuietly(backupWriterContext.getSourceSession());
        super.close();
    }
}