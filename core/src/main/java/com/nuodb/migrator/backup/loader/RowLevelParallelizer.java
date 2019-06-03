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

import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.utils.concurrent.ForkJoinPool;

import java.util.Iterator;
import java.util.Map;

import static java.lang.Long.parseLong;
import static java.lang.Math.*;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Forking on row level where the number of workers is proportional to the size
 * of the largest row set. Notice row level parallelization may (and typically
 * does) reorder of the rows in the loaded table.
 *
 * @author Sergey Bushik
 */
public class RowLevelParallelizer implements Parallelizer {

    public static final String ATTRIBUTE_MIN_ROWS_PER_THREAD = "min.rows.per.thread";
    public static final String ATTRIBUTE_MAX_ROWS_PER_THREAD = "max.rows.per.thread";
    public static final long MIN_ROWS_PER_THREAD = 100000L;
    public static final long MAX_ROWS_PER_THREAD = 0L;

    private long minRowsPerThread = MIN_ROWS_PER_THREAD;
    private long maxRowsPerThread = MAX_ROWS_PER_THREAD;

    @Override
    public void setAttributes(Map<String, Object> attributes) {
        Object minRowsPerThreadValue = attributes.get(ATTRIBUTE_MIN_ROWS_PER_THREAD);
        if (minRowsPerThreadValue instanceof String && !isEmpty((String) minRowsPerThreadValue)) {
            setMinRowsPerThread(parseLong((String) minRowsPerThreadValue));
        }
        Object maxRowsPerThreadValue = attributes.get(ATTRIBUTE_MAX_ROWS_PER_THREAD);
        if (maxRowsPerThreadValue instanceof String && !isEmpty((String) maxRowsPerThreadValue)) {
            setMaxRowsPerThread(parseLong((String) maxRowsPerThreadValue));
        }
    }

    @Override
    public int getThreads(LoadTable loadTable, BackupLoaderContext backupLoaderContext) {
        BackupOps backupOps = backupLoaderContext.getBackupOps();
        RowSet rowSet = loadTable.getRowSet();
        long rowSetSize = rowSet.getSize(backupOps);
        long maxRowSetSize = 0L;
        for (Iterator<LoadTable> iterator = backupLoaderContext.getLoadTables().iterator(); iterator.hasNext();) {
            maxRowSetSize = max(iterator.next().getRowSet().getSize(backupOps), maxRowSetSize);
        }
        long threads = getThreads(backupLoaderContext);
        long minThreadsPerRowSet = getMinThreadsPerRowSet(loadTable, backupLoaderContext);
        long maxThreadsPerRowSet = getMaxThreadsPerRowSet(loadTable, backupLoaderContext);
        return (int) min(max(round(rowSetSize / (double) maxRowSetSize * threads), minThreadsPerRowSet),
                maxThreadsPerRowSet);
    }

    protected int getThreads(BackupLoaderContext backupLoaderContext) {
        ForkJoinPool forkJoinPool = (ForkJoinPool) backupLoaderContext.getExecutorService();
        return forkJoinPool.getParallelism();
    }

    protected long getMinThreadsPerRowSet(LoadTable loadTable, BackupLoaderContext backupLoaderContext) {
        long rowCount = loadTable.getRowSet().getRowCount();
        long minRowsPerThread = getMinRowsPerThread();
        return max(minRowsPerThread != 0 ? round(rowCount / (double) minRowsPerThread) : 1, 1);
    }

    protected long getMaxThreadsPerRowSet(LoadTable loadTable, BackupLoaderContext backupLoaderContext) {
        long rowCount = loadTable.getRowSet().getRowCount();
        int threads = getThreads(backupLoaderContext);
        return min(getMaxRowsPerThread() != 0 ? round(rowCount / (double) getMaxRowsPerThread()) : threads, threads);
    }

    public long getMinRowsPerThread() {
        return minRowsPerThread;
    }

    public void setMinRowsPerThread(long minRowsPerThread) {
        this.minRowsPerThread = minRowsPerThread;
    }

    public long getMaxRowsPerThread() {
        return maxRowsPerThread;
    }

    public void setMaxRowsPerThread(long maxRowsPerThread) {
        this.maxRowsPerThread = maxRowsPerThread;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RowLevelParallelizer that = (RowLevelParallelizer) o;

        if (maxRowsPerThread != that.maxRowsPerThread)
            return false;
        if (minRowsPerThread != that.minRowsPerThread)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (minRowsPerThread ^ (minRowsPerThread >>> 32));
        result = 31 * result + (int) (maxRowsPerThread ^ (maxRowsPerThread >>> 32));
        return result;
    }
}
