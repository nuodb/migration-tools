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
package com.nuodb.migrator.backup.writer;

import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.Column;
import com.nuodb.migrator.backup.QueryRowSet;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.format.Output;
import com.nuodb.migrator.backup.format.value.Row;
import com.nuodb.migrator.backup.format.value.Value;
import com.nuodb.migrator.backup.format.value.ValueHandle;
import com.nuodb.migrator.backup.format.value.ValueHandleList;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.FetchMode;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.session.WorkForkJoinTaskBase;
import com.nuodb.migrator.jdbc.split.QuerySplit;
import com.nuodb.migrator.utils.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.indexOf;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.BackupMessages.WRITE_QUERY_WORK;
import static com.nuodb.migrator.backup.format.value.ValueHandleListBuilder.newBuilder;
import static com.nuodb.migrator.context.ContextUtils.getMessage;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.jdbc.model.FieldFactory.newFieldList;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static com.nuodb.migrator.utils.Predicates.equalTo;
import static com.nuodb.migrator.utils.Predicates.instanceOf;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.lowerCase;

/**
 * Work executed by a thread, which exports table rows to a row set. Row set is
 * split into chunks if specified.
 *
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class WriteQueryWork extends WorkForkJoinTaskBase {

    private static final String QUERY = "query";

    private final BackupWriterManager backupWriterManager;
    private final WriteQuery writeQuery;
    private final QuerySplit querySplit;
    private final boolean hasNextQuerySplit;

    private ResultSet resultSet;
    private Output output;
    private Collection<Chunk> chunks;
    private BackupWriterContext backupWriterContext;
    private ValueHandleList valueHandleList;

    public WriteQueryWork(WriteQuery writeQuery, QuerySplit querySplit, boolean hasNextQuerySplit,
            BackupWriterManager backupWriterManager) {
        super(backupWriterManager, backupWriterManager.getBackupWriterContext().getSourceSessionFactory());
        this.writeQuery = writeQuery;
        this.querySplit = querySplit;
        this.hasNextQuerySplit = hasNextQuerySplit;
        this.backupWriterManager = backupWriterManager;
    }

    @Override
    public String getName() {
        return getMessage(WRITE_QUERY_WORK, getRowSetName());
    }

    @Override
    public void init() throws Exception {
        backupWriterContext = backupWriterManager.getBackupWriterContext();

        final Dialect dialect = getSession().getDialect();
        resultSet = querySplit.getResultSet(getSession().getConnection(), new StatementCallback() {
            @Override
            public void executeStatement(Statement statement) throws SQLException {
                boolean stream = writeQuery.getColumns() != null;
                dialect.setFetchMode(statement, new FetchMode(stream));
            }
        });

        Collection<? extends Field> fields = writeQuery.getColumns() != null ? writeQuery.getColumns()
                : newFieldList(resultSet);

        valueHandleList = newBuilder(getSession().getConnection(), resultSet).withDialect(dialect).withFields(fields)
                .withTimeZone(backupWriterContext.getTimeZone())
                .withValueFormatRegistry(backupWriterContext.getValueFormatRegistry()).build();

        RowSet rowSet = writeQuery.getRowSet();
        if (isEmpty(rowSet.getColumns())) {
            Collection<Column> columns = newArrayList();
            for (ValueHandle valueHandle : valueHandleList) {
                columns.add(new Column(valueHandle.getName(), valueHandle.getValueType()));
            }
            rowSet.setColumns(columns);
        }
        rowSet.setName(getRowSetName());

        output = backupWriterContext.getFormatFactory().createOutput(backupWriterContext.getFormat(),
                backupWriterContext.getFormatAttributes());
        output.setRowSet(rowSet);

        chunks = newArrayList();
    }

    @Override
    public void execute() throws Exception {
        backupWriterManager.writeStart(this, writeQuery);
        ResultSet resultSet = getResultSet();
        Output output = getOutput();
        Chunk chunk = null;
        long number = 0;
        Value[] values = new Value[valueHandleList.size()];
        while (backupWriterManager.canExecute(this) && resultSet.next()) {
            if (chunk == null) {
                writeStart(chunk = addChunk());
            }
            if (!output.canWrite()) {
                writeEnd(chunk);
                writeStart(chunk = addChunk());
            }
            Row row = new Row(chunk, values, number);
            int index = 0;
            for (ValueHandle valueHandle : valueHandleList) {
                values[index++] = valueHandle.getValueFormat().getValue(valueHandle.getJdbcValueAccess(),
                        valueHandle.getJdbcValueAccessOptions());
            }
            output.writeValues(values);
            chunk.incrementRowCount();
            backupWriterManager.writeRow(this, writeQuery, row);
        }
        if (chunk != null) {
            writeEnd(chunk);
        }
        backupWriterManager.writeEnd(this, writeQuery);
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeQuietly(resultSet);
    }

    protected void writeStart(Chunk chunk) throws Exception {
        output.setOutputStream(backupWriterContext.getBackupOps().openOutput(chunk.getName()));
        output.init();
        output.writeStart();
        backupWriterManager.writeStart(this, writeQuery, chunk);
    }

    protected void writeEnd(Chunk chunk) throws Exception {
        output.writeEnd();
        output.close();
        backupWriterManager.writeEnd(this, writeQuery, chunk);
    }

    protected Chunk addChunk() {
        Chunk chunk = createChunk(chunks.size());
        chunks.add(chunk);
        return chunk;
    }

    protected Chunk createChunk(int chunkIndex) {
        Chunk chunk = new Chunk();
        chunk.setName(getChunkName(chunkIndex));
        return chunk;
    }

    protected String getChunkName(int chunkIndex) {
        Collection names = newArrayList(getRowSetName());
        int splitIndex = getQuerySplit().getSplitIndex();
        if (splitIndex != 0 || isHasNextQuerySplit()) {
            names.add(splitIndex + 1);
        }
        if (chunkIndex > 0) {
            names.add(chunkIndex + 1);
        }
        names.add(backupWriterContext.getFormat());
        return lowerCase(StringUtils.join(names, "."));
    }

    protected String getRowSetName() {
        String rowSetName;
        if (writeQuery instanceof WriteTable) {
            Table table = ((WriteTable) writeQuery).getTable();
            rowSetName = table.getQualifiedName(null);
        } else {
            RowSet rowSet = writeQuery.getRowSet();
            int rowSetIndex = indexOf(filter(rowSet.getBackup().getRowSets(), instanceOf(QueryRowSet.class)),
                    equalTo(rowSet));
            rowSetName = StringUtils.join(asList(QUERY, rowSetIndex + 1), "-");
        }
        return lowerCase(rowSetName);
    }

    public WriteQuery getWriteQuery() {
        return writeQuery;
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

    protected Output getOutput() {
        return output;
    }

    protected Collection<Chunk> getChunks() {
        return chunks;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this, asList("writeQuery", "querySplit", "hasNextQuerySplit"));
    }
}
