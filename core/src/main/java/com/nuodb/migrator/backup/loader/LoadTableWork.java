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

import com.nuodb.migrator.backup.format.value.RowReader;
import com.nuodb.migrator.jdbc.session.WorkForkJoinTaskBase;
import org.slf4j.Logger;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.BackupMessages.LOAD_TABLE_WORK;
import static com.nuodb.migrator.backup.format.value.RowReaders.newSequentialRowReader;
import static com.nuodb.migrator.backup.format.value.RowReaders.newSynchronizedRowReader;
import static com.nuodb.migrator.context.ContextUtils.getMessage;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Represents parallelized table loading
 *
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class LoadTableWork extends WorkForkJoinTaskBase {

    private transient Logger logger = getLogger(getClass());

    private LoadTable loadTable;
    private BackupLoaderManager backupLoaderManager;
    private RowReader rowReader;

    public LoadTableWork(LoadTable loadTable, BackupLoaderManager backupLoaderManager) {
        super(backupLoaderManager, backupLoaderManager.getBackupLoaderContext().getTargetSession());
        this.loadTable = loadTable;
        this.backupLoaderManager = backupLoaderManager;
    }

    @Override
    public String getName() {
        return getMessage(LOAD_TABLE_WORK, loadTable.getRowSet().getName());
    }

    @Override
    protected void init() throws Exception {
        BackupLoaderContext backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        RowReader rowReader = newSequentialRowReader(loadTable.getRowSet(), backupLoaderContext.getBackupOps(),
                backupLoaderContext.getFormatFactory(), backupLoaderContext.getFormatAttributes());
        int threads = loadTable.getThreads();
        if (threads > 1) {
            rowReader = newSynchronizedRowReader(rowReader);
        }
        this.rowReader = rowReader;
    }

    @Override
    public void execute() throws Exception {
        Collection<LoadTableForkWork> loadTableForkWorks = newArrayList();
        for (int thread = 0; thread < loadTable.getThreads(); thread++) {
            LoadTableForkWork loadTableForkWork = new LoadTableForkWork(loadTable, rowReader, thread,
                    backupLoaderManager);
            loadTableForkWork.fork();
            loadTableForkWorks.add(loadTableForkWork);
        }
        for (LoadTableForkWork loadTableForkWork : loadTableForkWorks) {
            loadTableForkWork.join();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeQuietly(rowReader);
    }

    public LoadTable getLoadTable() {
        return loadTable;
    }
}
