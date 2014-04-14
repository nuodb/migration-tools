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

import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.jdbc.session.SimpleWorkManager;
import com.nuodb.migrator.jdbc.session.Work;

/**
 * @author Sergey Bushik
 */
public class SimpleBackupLoaderManager extends SimpleWorkManager<BackupLoaderListener>
        implements BackupLoaderManager {

    private Long deltaRowCount;

    @Override
    public boolean canLoad(Work work, LoadRowSet loadRowSet) {
        return getFailures().isEmpty();
    }

    @Override
    public void loadStart(Work work, LoadRowSet loadRowSet) {
        if (hasListeners()) {
            onLoadStart(new LoadRowSetEvent(work, loadRowSet));
        }
    }

    @Override
    public void loadStart(Work work, LoadRowSet loadRowSet, Chunk chunk) {
        if (hasListeners()) {
            onLoadStart(new LoadRowSetEvent(work, loadRowSet, chunk));
        }
    }

    protected void onLoadStart(LoadRowSetEvent event) {
        for (BackupLoaderListener listener : getListeners()) {
            listener.onLoadStart(event);
        }
    }

    @Override
    public void loadRow(Work work, LoadRowSet loadRowSet, Chunk chunk) {
        Long deltaRowCount = getDeltaRowCount();
        if (hasListeners() &&  (deltaRowCount != null && chunk.getRowCount() % deltaRowCount == 0)) {
            onLoadRow(new LoadRowSetEvent(work, loadRowSet, chunk));
        }
    }

    protected void onLoadRow(LoadRowSetEvent event) {
        for (BackupLoaderListener listener : getListeners()) {
            listener.onLoadRow(event);
        }
    }

    @Override
    public void loadEnd(Work work, LoadRowSet loadRowSet, Chunk chunk) {
        if (hasListeners()) {
            onLoadEnd(new LoadRowSetEvent(work, loadRowSet, chunk));
        }
    }

    @Override
    public void loadEnd(Work work, LoadRowSet loadRowSet) {
        if (hasListeners()) {
            onLoadEnd(new LoadRowSetEvent(work, loadRowSet));
        }
    }

    protected void onLoadEnd(LoadRowSetEvent event) {
        for (BackupLoaderListener listener : getListeners()) {
            listener.onLoadEnd(event);
        }
    }

    @Override
    public Long getDeltaRowCount() {
        return deltaRowCount;
    }

    @Override
    public void setDeltaRowCount(Long deltaRowCount) {
        this.deltaRowCount = deltaRowCount;
    }
}
