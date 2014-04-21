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

import com.google.common.base.Function;
import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.Column;
import com.nuodb.migrator.backup.format.InputFormat;
import com.nuodb.migrator.backup.format.value.ValueHandleListBuilder;
import com.nuodb.migrator.jdbc.commit.BatchCommitStrategy;
import com.nuodb.migrator.jdbc.commit.CommitExecutor;
import com.nuodb.migrator.jdbc.commit.CommitStrategy;
import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.session.WorkBase;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.util.Iterator;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.format.value.ValueHandleListBuilder.newBuilder;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class LoadRowSetWork extends WorkBase {

    protected transient Logger logger = getLogger(getClass());

    private LoadRowSet loadRowSet;
    private BackupLoaderManager backupLoaderManager;

    private BackupLoaderContext backupLoaderContext;
    private PreparedStatement statement;
    private CommitExecutor commitExecutor;

    public LoadRowSetWork(LoadRowSet loadRowSet, BackupLoaderManager backupLoaderManager) {
        this.loadRowSet = loadRowSet;
        this.backupLoaderManager = backupLoaderManager;
    }

    @Override
    protected void init() throws Exception {
        backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        statement = getSession().getConnection().prepareStatement(
                loadRowSet.getQuery().toString());
        CommitStrategy commitStrategy = backupLoaderContext.getCommitStrategy() != null ?
                backupLoaderContext.getCommitStrategy() : BatchCommitStrategy.INSTANCE;
        commitExecutor = commitStrategy.createCommitExecutor(statement, loadRowSet.getQuery());
    }

    @Override
    public void execute() throws Exception {
        LoadRowSet loadRowSet = getLoadRowSet();
        BackupLoaderManager backupLoaderManager = getBackupLoaderManager();
        backupLoaderManager.loadStart(this, loadRowSet);
        for (Iterator<Chunk> chunks = loadRowSet.getRowSet().getChunks().iterator();
             backupLoaderManager.canLoad(this, loadRowSet) && chunks.hasNext(); ) {
            Chunk chunk = chunks.next();
            backupLoaderManager.loadStart(this, loadRowSet, chunk);
            InputFormat inputFormat = openInput(chunk.getName());
            inputFormat.init();
            if (logger.isTraceEnabled()) {
                logger.trace(format("Loading %d rows from %s chunk to %s table",
                        chunk.getRowCount(), chunk.getName(), loadRowSet.getTable().getQualifiedName(null)));
            }
            inputFormat.readStart();
            long row = 0;
            try {
                while (inputFormat.read() && backupLoaderManager.canLoad(this, loadRowSet)) {
                    commitExecutor.execute();
                    row++;
                    backupLoaderManager.loadRow(this, loadRowSet, chunk);
                }
                commitExecutor.finish();
            } catch (Exception exception) {
                throw new BackupLoaderException(format("Error loading row %d from %s chunk to %s table",
                        row + 1, chunk.getName(), loadRowSet.getTable().getQualifiedName()), exception);
            }
            inputFormat.readEnd();
            inputFormat.close();
            backupLoaderManager.loadEnd(this, loadRowSet, chunk);
            if (logger.isTraceEnabled()) {
                logger.trace(format("Chunk %s loaded", chunk.getName()));
            }
        }
        backupLoaderManager.loadEnd(this, loadRowSet);
    }

    protected InputFormat openInput(String input) {
        String format = backupLoaderContext.getBackup().getFormat();
        InputFormat inputFormat = backupLoaderContext.getFormatFactory().createInputFormat(format,
                backupLoaderContext.getFormatAttributes());
        inputFormat.setRowSet(getLoadRowSet().getRowSet());
        inputFormat.setInputStream(backupLoaderContext.getBackupOps().openInput(input));
        ValueHandleListBuilder builder = newBuilder(getSession().getConnection(), statement);
        builder.withDialect(getSession().getDialect());
        builder.withFields(newArrayList(transform(getLoadRowSet().getRowSet().getColumns(),
                new Function<Column, Field>() {
                    @Override
                    public Field apply(Column column) {
                        return getLoadRowSet().getTable().getColumn(column.getName());
                    }
                })));
        builder.withTimeZone(backupLoaderContext.getTimeZone());
        builder.withValueFormatRegistry(backupLoaderContext.getValueFormatRegistry());
        inputFormat.setValueHandleList(builder.build());
        return inputFormat;
    }

    public BackupLoaderManager getBackupLoaderManager() {
        return backupLoaderManager;
    }

    public BackupLoaderContext getBackupLoaderContext() {
        return backupLoaderContext;
    }

    public LoadRowSet getLoadRowSet() {
        return loadRowSet;
    }

    @Override
    public void close() throws Exception {
        closeQuietly(statement);
        super.close();
    }
}
