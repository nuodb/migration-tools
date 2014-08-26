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
package com.nuodb.migrator.backup.loader;

import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.utils.concurrent.ForkJoinPool;

import java.util.Iterator;

import static java.lang.Math.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class Parallelizers {

    /**
     * Forking on table level, which means that one thread per table is used
     */
    public static Parallelizer TABLE_LEVEL = new Parallelizer() {
        @Override
        public int getThreads(LoadTable loadTable, BackupLoaderContext backupLoaderContext) {
            return 1;
        }
    };

    /**
     * Forking on row level where the number of workers is based on the weight of row set to the total size of loaded
     * tables. Notice row level parallelization may (and typically does) reorder of the rows in the loaded table.
     */
    public static Parallelizer ROW_LEVEL = new Parallelizer() {
        @Override
        public int getThreads(LoadTable loadTable, BackupLoaderContext backupLoaderContext) {
            ForkJoinPool forkJoinPool = (ForkJoinPool) backupLoaderContext.getExecutorService();
            BackupOps backupOps = backupLoaderContext.getBackupOps();
            long rowSetSize = loadTable.getRowSet().getSize(backupOps);
            long backupSize = 0L;
            for (Iterator<LoadTable> iterator = backupLoaderContext.getLoadTables().iterator(); iterator.hasNext(); ) {
                backupSize += iterator.next().getRowSet().getSize(backupOps);
            }
            int threads = forkJoinPool.getParallelism();
            return (int) min(max(round(rowSetSize / (double) backupSize * threads), 1), threads);
        }
    };
}
