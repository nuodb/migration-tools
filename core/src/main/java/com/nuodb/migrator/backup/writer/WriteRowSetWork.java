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

import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.Column;
import com.nuodb.migrator.backup.QueryRowSet;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.format.OutputFormat;
import com.nuodb.migrator.backup.format.value.ValueHandle;
import com.nuodb.migrator.backup.format.value.ValueHandleList;
import com.nuodb.migrator.jdbc.JdbcUtils;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.session.WorkBase;
import com.nuodb.migrator.jdbc.split.QuerySplit;
import com.nuodb.migrator.utils.ObjectUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.indexOf;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.format.value.ValueHandleListBuilder.newBuilder;
import static com.nuodb.migrator.backup.format.value.ValueType.toAlias;
import static com.nuodb.migrator.jdbc.model.FieldFactory.newFieldList;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static com.nuodb.migrator.utils.Predicates.equalTo;
import static com.nuodb.migrator.utils.Predicates.instanceOf;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.lowerCase;

/**
 * Work executed by a thread, which exports table rows to a row set. Row set is split into chunks if specified.
 *
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class WriteRowSetWork extends WorkBase {

    private static final String QUERY = "query";

    private final BackupWriterManager backupWriterManager;
    private final WriteRowSet writeRowSet;
    private final QuerySplit querySplit;
    private final boolean hasNextQuerySplit;

    private ResultSet resultSet;
    private ValueHandleList valueHandleList;
    private OutputFormat outputFormat;
    private Collection<Chunk> chunks;
    private BackupWriterContext backupWriterContext;

    public WriteRowSetWork(WriteRowSet writeRowSet, QuerySplit querySplit, boolean hasNextQuerySplit,
                           BackupWriterManager backupWriterManager) {
        this.writeRowSet = writeRowSet;
        this.querySplit = querySplit;
        this.hasNextQuerySplit = hasNextQuerySplit;
        this.backupWriterManager = backupWriterManager;
    }

    @Override
    public void init() throws Exception {
        backupWriterContext = backupWriterManager.getBackupWriterContext();

        final Dialect dialect = getSession().getDialect();
        resultSet = querySplit.getResultSet(getSession().getConnection(), new StatementCallback() {
            @Override
            public void executeStatement(Statement statement) throws SQLException {
                dialect.setStreamResults(statement, writeRowSet.getColumns() != null);
            }
        });

        Collection<? extends Field> fields = writeRowSet.getColumns() != null ?
                writeRowSet.getColumns() : newFieldList(resultSet);

        valueHandleList = newBuilder(getSession().getConnection(), resultSet).
                withDialect(dialect).withFields(fields).
                withTimeZone(backupWriterContext.getTimeZone()).
                withValueFormatRegistry(backupWriterContext.getValueFormatRegistry()).build();

        RowSet rowSet = writeRowSet.getRowSet();
        if (isEmpty(rowSet.getColumns())) {
            Collection<Column> columns = newArrayList();
            for (ValueHandle valueHandle : valueHandleList) {
                columns.add(new Column(
                        valueHandle.getName(), toAlias(valueHandle.getValueType())));
            }
            rowSet.setColumns(columns);
        }

        outputFormat = backupWriterContext.getFormatFactory().createOutputFormat(
                backupWriterContext.getFormat(), backupWriterContext.getFormatAttributes());
        outputFormat.setRowSet(rowSet);
        outputFormat.setValueHandleList(valueHandleList);

        chunks = newArrayList();
        if (rowSet.getName() == null) {
            rowSet.setName(createRowSetName());
        }
    }

    @Override
    public void execute() throws Exception {
        backupWriterManager.writeStart(this, writeRowSet);

        ResultSet resultSet = getResultSet();
        OutputFormat outputFormat = getOutputFormat();
        Chunk chunk = null;
        while (backupWriterManager.canWrite(this, writeRowSet) && resultSet.next()) {
            if (chunk == null) {
                exportStart(chunk = addChunk());
            }
            if (!outputFormat.canWrite()) {
                exportEnd(chunk);
                exportStart(chunk = addChunk());
            }
            outputFormat.write();
            chunk.incrementRowCount();
            backupWriterManager.writeRow(this, writeRowSet, chunk);
        }
        if (chunk != null) {
            exportEnd(chunk);
        }
        backupWriterManager.writeEnd(this, writeRowSet);
    }

    @Override
    public void close() throws Exception {
        JdbcUtils.closeQuietly(resultSet);
    }

    protected void exportStart(Chunk chunk) throws Exception {
        outputFormat.setOutputStream(backupWriterContext.getBackupOps().openOutput(chunk.getName()));
        outputFormat.init();
        outputFormat.writeStart();
        backupWriterManager.writeStart(this, writeRowSet, chunk);
    }

    protected void exportEnd(Chunk chunk) throws Exception {
        outputFormat.writeEnd();
        outputFormat.close();
        backupWriterManager.writeEnd(this, writeRowSet, chunk);
    }

    protected Chunk addChunk() {
        Chunk chunk = createChunk(chunks.size());
        chunks.add(chunk);
        return chunk;
    }

    protected Chunk createChunk(int chunkIndex) {
        Chunk chunk = new Chunk();
        chunk.setName(createChunkName(chunkIndex));
        return chunk;
    }

    protected String createChunkName(int chunkIndex) {
        Collection names = newArrayList(createRowSetName());
        int splitIndex = getQuerySplit().getSplitIndex();
        if (splitIndex != 0 || isHasNextQuerySplit()) {
            names.add(splitIndex + 1);
        }
        if (chunkIndex > 0) {
            names.add(chunkIndex + 1);
        }
        names.add(backupWriterContext.getFormat());
        return lowerCase(join(names, "."));
    }

    protected String createRowSetName() {
        String rowSetName;
        if (writeRowSet instanceof WriteTableRowSet) {
            Table table = ((WriteTableRowSet) writeRowSet).getTable();
            rowSetName = table.getQualifiedName(null);
        } else {
            RowSet rowSet = writeRowSet.getRowSet();
            int rowSetIndex = indexOf(filter(rowSet.getBackup().getRowSets(),
                    instanceOf(QueryRowSet.class)), equalTo(rowSet));
            rowSetName = join(asList(QUERY, rowSetIndex + 1), "-");
        }
        return lowerCase(rowSetName);
    }

    public BackupWriterContext getBackupWriterContext() {
        return backupWriterContext;
    }

    public WriteRowSet getWriteRowSet() {
        return writeRowSet;
    }

    public QuerySplit getQuerySplit() {
        return querySplit;
    }

    public boolean isHasNextQuerySplit() {
        return hasNextQuerySplit;
    }

    protected ResultSet getResultSet() {
        return resultSet;
    }

    protected ValueHandleList getValueHandleList() {
        return valueHandleList;
    }

    protected OutputFormat getOutputFormat() {
        return outputFormat;
    }

    protected Collection<Chunk> getChunks() {
        return chunks;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this,
                asList("queryDesc", "querySplit", "hasNextQuerySplit"));
    }
}
