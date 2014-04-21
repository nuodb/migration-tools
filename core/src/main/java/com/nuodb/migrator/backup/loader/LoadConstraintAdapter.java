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

import com.google.common.collect.Multimap;
import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.session.Work;
import com.nuodb.migrator.jdbc.session.WorkEvent;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.ArrayListMultimap.create;
import static com.google.common.collect.Multimaps.synchronizedListMultimap;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;

/**
 * @author Sergey Bushik
 */
public class LoadConstraintAdapter extends BackupLoaderAdapter {

    private BackupLoader backupLoader;
    private BackupLoaderManager backupLoaderManager;
    private Multimap<Table, LoadConstraint> loadIndexes;
    private Multimap<Table, LoadConstraint> loadForeignKeys;
    private AtomicBoolean loadForeignKeysStart = new AtomicBoolean();

    public LoadConstraintAdapter(BackupLoader backupLoader, BackupLoaderManager backupLoaderManager) {
        this.backupLoader = backupLoader;
        this.backupLoaderManager = backupLoaderManager;

        LoadConstraints loadConstraints = backupLoaderManager.
                getBackupLoaderContext().getLoadConstraints();
        this.loadIndexes = synchronizedListMultimap(create(
                loadConstraints.getLoadConstraints(INDEX, PRIMARY_KEY)));
        this.loadForeignKeys = synchronizedListMultimap(create(
                loadConstraints.getLoadConstraints(FOREIGN_KEY)));
    }

    /**
     * Monitors table loaded event and performs indexes & primary key loading
     *
     * @param event load row set event
     */
    @Override
    public void onLoadEnd(LoadRowSetEvent event) {
        try {
            Table table = backupLoader.getTable(event.getLoadRowSet(),
                    backupLoaderManager.getBackupLoaderContext());
            if (event.getChunk() == null && table != null) {
                backupLoader.loadConstraints(loadIndexes.get(table), backupLoaderManager);
            }
        } catch (MigratorException exception) {
            backupLoaderManager.loadFailed();
            throw exception;
        } catch (Exception exception) {
            backupLoaderManager.loadFailed();
            throw new BackupLoaderException(exception);
        }
    }

    /**
     * Tracks completion of load constraint work and updates list of queued indexes, primary keys & foreign keys. Once
     * indexes are loaded foreign keys are started, eventually when all constraints are loaded corresponding signal will
     * be called on sync object.
     *
     * @param event defining work completion
     */
    @Override
    public void onExecuteEnd(WorkEvent event) {
        try {
            Work work = event.getWork();
            if (work instanceof LoadConstraintWork) {
                LoadConstraintWork loadConstraintWork = (LoadConstraintWork) work;
                LoadConstraint loadConstraint = loadConstraintWork.getLoadConstraint();
                loadIndexes.remove(loadConstraint.getTable(), loadConstraint);
                loadForeignKeys.remove(loadConstraint.getTable(), loadConstraint);
            }
            loadForeignKeys();
            loadConstraintsDone();
        } catch (MigratorException exception) {
            backupLoaderManager.loadFailed();
            throw exception;
        } catch (Exception exception) {
            backupLoaderManager.loadFailed();
            throw new BackupLoaderException(exception);
        }
    }

    protected void loadForeignKeys() {
        if (loadIndexes.isEmpty() && loadForeignKeysStart.compareAndSet(false, true)) {
            for (LoadConstraint loadForeignKey : loadForeignKeys.values()) {
                backupLoader.loadConstraint(loadForeignKey, backupLoaderManager);
            }
        }
    }

    protected void loadConstraintsDone() {
        // once all indexes & foreign keys are loaded shutdown pool and await for termination
        if (loadIndexes.isEmpty() && loadForeignKeys.isEmpty()) {
            backupLoaderManager.loadConstraintsDone();
        }
    }
}