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
import com.nuodb.migrator.backup.Column;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.format.value.*;
import com.nuodb.migrator.jdbc.commit.BatchCommitStrategy;
import com.nuodb.migrator.jdbc.commit.CommitExecutor;
import com.nuodb.migrator.jdbc.commit.CommitStrategy;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.session.WorkForkJoinTaskBase;
import org.slf4j.Logger;

import java.sql.PreparedStatement;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.format.value.ValueHandleListBuilder.newBuilder;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Represents table loading parallelization on row level
 *
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class LoadTableRowsWork extends WorkForkJoinTaskBase {

    protected transient Logger logger = getLogger(getClass());

    private int thread;
    private LoadTable loadTable;
    private RowReader rowReader;
    private BackupLoaderManager backupLoaderManager;

    private BackupLoaderContext backupLoaderContext;
    private PreparedStatement statement;
    private CommitExecutor commitExecutor;
    private ValueHandleList valueHandleList;

    public LoadTableRowsWork(int thread, RowReader rowReader, LoadTable loadTable,
                             BackupLoaderManager backupLoaderManager) {
        super(backupLoaderManager, backupLoaderManager.getBackupLoaderContext().getTargetSessionFactory());
        this.rowReader = rowReader;
        this.thread = thread;
        this.loadTable = loadTable;
        this.backupLoaderManager = backupLoaderManager;
    }

    @Override
    public String getName() {
        RowSet rowSet = loadTable.getRowSet();
        return format("Load from %s thread #%d", rowSet.getName(), getThread());
    }

    @Override
    protected void init() throws Exception {
        backupLoaderContext = backupLoaderManager.getBackupLoaderContext();
        statement = getSession().getConnection().prepareStatement(
                loadTable.getQuery().toString());
        CommitStrategy commitStrategy = backupLoaderContext.getCommitStrategy() != null ?
                backupLoaderContext.getCommitStrategy() : BatchCommitStrategy.INSTANCE;
        commitExecutor = commitStrategy.createCommitExecutor(statement, loadTable.getQuery());
    }

    @Override
    public void execute() throws Exception {
        Row row;
        while ((row = rowReader.readRow()) != null && backupLoaderManager.canExecute(this)) {
            backupLoaderManager.beforeLoadRow(this, loadTable, row);
            int index = 0;
            Value[] values = row.getValues();
            initValueHandleList();
            for (ValueHandle valueHandle : valueHandleList) {
                valueHandle.getValueFormat().setValue(values[index++],
                        valueHandle.getJdbcValueAccess(), valueHandle.getJdbcValueAccessOptions());
            }
            commitExecutor.execute();
            backupLoaderManager.afterLoadRow(this, loadTable, row);
        }
    }

    protected void initValueHandleList() {
        if (valueHandleList == null) {
            ValueHandleListBuilder builder = newBuilder(getSession().getConnection(), statement);
            builder.withDialect(getSession().getDialect());
            builder.withFields(newArrayList(transform(loadTable.getRowSet().getColumns(),
                    new Function<Column, Field>() {
                        @Override
                        public Field apply(Column column) {
                            return loadTable.getTable().getColumn(column.getName());
                        }
                    })));
            builder.withTimeZone(backupLoaderContext.getTimeZone());
            builder.withValueFormatRegistry(backupLoaderContext.getValueFormatRegistry());
            valueHandleList = builder.build();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeQuietly(statement);
    }

    public int getThread() {
        return thread;
    }

    public LoadTable getLoadTable() {
        return loadTable;
    }
}