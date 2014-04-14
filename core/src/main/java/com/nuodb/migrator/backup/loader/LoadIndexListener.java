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

import com.nuodb.migrator.MigratorException;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.synchronizedList;

/**
 * @author Sergey Bushik
 */
public class LoadIndexListener extends BackupLoaderAdapter {

    private BackupLoader backupLoader;
    private BackupLoaderManager backupLoaderManager;
    private Collection<LoadIndex> loadIndexes;

    public LoadIndexListener(BackupLoader backupLoader, BackupLoaderManager backupLoaderManager) {
        this.backupLoader = backupLoader;
        this.backupLoaderManager = backupLoaderManager;

        BackupLoaderContext backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        this.loadIndexes = synchronizedList(newArrayList(backupLoaderContext.getLoadIndexes()));
    }

    @Override
    public void onLoadEnd(LoadRowSetEvent event) {
        Table table = backupLoader.getTable(event.getLoadRowSet(),
                backupLoaderManager.getBackupLoaderContext());
        if (event.getChunk() == null && table != null) {
            try {
                LoadIndex loadIndex = new LoadIndex(table);
                backupLoader.loadIndex(loadIndex, backupLoaderManager);
                loadIndexes.remove(loadIndex);
                // once all indexes are loaded shutdown pool and await for termination
                if (loadIndexes.isEmpty()) {
                    backupLoaderManager.loadSchemaIndexesDone();
                }
            } catch (MigratorException exception) {
                backupLoaderManager.loadFailed();
                throw exception;
            } catch (Exception exception) {
                backupLoaderManager.loadFailed();
                throw new BackupLoaderException(exception);
            }
        }
    }
}
