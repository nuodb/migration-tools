/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.migrator.dump;

import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.QueryRowSet;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.format.OutputFormat;
import com.nuodb.migrator.backup.format.value.ValueHandleList;
import com.nuodb.migrator.jdbc.JdbcUtils;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.session.WorkBase;
import com.nuodb.migrator.jdbc.split.QuerySplit;
import com.nuodb.migrator.utils.ObjectUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.indexOf;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.format.value.ValueHandleListBuilder.newBuilder;
import static com.nuodb.migrator.jdbc.model.ColumnFactory.createColumnList;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.lowerCase;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DumpQueryWork extends WorkBase {

    private static final String QUERY = "query";

    private final DumpQueryContext dumpQueryContext;
    private final DumpQueryManager dumpQueryManager;
    private final DumpQuery dumpQuery;
    private final QuerySplit querySplit;
    private final boolean hasNextQuerySplit;

    private ResultSet resultSet;
    private ValueHandleList valueHandleList;
    private OutputFormat outputFormat;
    private Collection<Chunk> chunks;

    public DumpQueryWork(DumpQueryContext dumpQueryContext, DumpQueryManager dumpQueryManager, DumpQuery dumpQuery,
                         QuerySplit querySplit, boolean hasNextQuerySplit) {
        this.dumpQueryContext = dumpQueryContext;
        this.dumpQueryManager = dumpQueryManager;
        this.dumpQuery = dumpQuery;
        this.querySplit = querySplit;
        this.hasNextQuerySplit = hasNextQuerySplit;
    }

    @Override
    public void init() throws Exception {
        final Dialect dialect = getSession().getDialect();
        resultSet = querySplit.getResultSet(getSession().getConnection(), new StatementCallback() {
            @Override
            public void executeStatement(Statement statement) throws SQLException {
                dialect.setStreamResults(statement, dumpQuery.getColumns() != null);
            }
        });

        valueHandleList = newBuilder(getSession().getConnection(), resultSet).
                withDialect(dialect).
                withColumns(dumpQuery.getColumns() != null ? dumpQuery.getColumns() : createColumnList(resultSet)).
                withTimeZone(dumpQueryContext.getTimeZone()).
                withValueFormatRegistry(dumpQueryContext.getValueFormatRegistry()).build();

        RowSet rowSet = dumpQuery.getRowSet();
        outputFormat = dumpQueryContext.getFormatFactory().createOutputFormat(
                dumpQueryContext.getFormat(), dumpQueryContext.getFormatAttributes());
        outputFormat.setRowSet(rowSet);
        outputFormat.setValueHandleList(valueHandleList);

        chunks = newArrayList();
        if (rowSet.getName() == null) {
            rowSet.setName(getRowSetName());
        }
    }

    @Override
    public void execute() throws Exception {
        DumpQueryManager dumpQueryManager = getDumpQueryManager();
        DumpQuery dumpQuery = getDumpQuery();
        dumpQueryManager.writeStart(dumpQuery, this);

        ResultSet resultSet = getResultSet();
        OutputFormat outputFormat = getOutputFormat();

        Chunk chunk = null;
        while (dumpQueryManager.canWrite(dumpQuery, this) && resultSet.next()) {
            if (chunk == null) {
                writeStart(chunk = addChunk());
            }
            if (!outputFormat.canWrite()) {
                writeEnd(chunk);
                writeStart(chunk = addChunk());
            }
            outputFormat.write();
            dumpQueryManager.write(dumpQuery, this, chunk);
        }
        if (chunk != null) {
            writeEnd(chunk);
        }
        dumpQueryManager.writeEnd(dumpQuery, this);
    }

    @Override
    public void close() throws Exception {
        JdbcUtils.close(resultSet);
    }

    protected void writeStart(Chunk chunk) throws Exception {
        outputFormat.setOutputStream(dumpQueryContext.getBackupManager().openOutput(chunk.getName()));
        outputFormat.init();
        outputFormat.writeStart();

        dumpQueryManager.writeStart(dumpQuery, this, chunk);
    }

    protected void writeEnd(Chunk chunk) throws Exception {
        outputFormat.writeEnd();
        outputFormat.close();
        dumpQueryManager.writeEnd(dumpQuery, this, chunk);
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
        Collection parts = newArrayList(getRowSetName());
        int splitIndex = getQuerySplit().getSplitIndex();
        if (splitIndex != 0 || isHasNextQuerySplit()) {
            parts.add(splitIndex + 1);
        }
        if (chunkIndex > 0) {
            parts.add(chunkIndex + 1);
        }
        parts.add(dumpQueryContext.getFormat());
        return lowerCase(join(parts, "."));
    }

    protected String getRowSetName() {
        String rowSetName;
        if (dumpQuery instanceof DumpTable) {
            Table table = ((DumpTable) dumpQuery).getTable();
            rowSetName = table.getQualifiedName(null);
        } else {
            RowSet rowSet = dumpQuery.getRowSet();
            int rowSetIndex = indexOf(filter(rowSet.getBackup().getRowSets(),
                    instanceOf(QueryRowSet.class)), equalTo(rowSet));
            rowSetName = join(asList(QUERY, rowSetIndex + 1), "-");
        }
        return lowerCase(rowSetName);
    }

    public DumpQueryManager getDumpQueryManager() {
        return dumpQueryManager;
    }

    public DumpQuery getDumpQuery() {
        return dumpQuery;
    }

    public QuerySplit getQuerySplit() {
        return querySplit;
    }

    public boolean isHasNextQuerySplit() {
        return hasNextQuerySplit;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ValueHandleList getValueHandleList() {
        return valueHandleList;
    }

    public void setValueHandleList(ValueHandleList valueHandleList) {
        this.valueHandleList = valueHandleList;
    }

    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(OutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

    public Collection<Chunk> getChunks() {
        return chunks;
    }

    public void setChunks(Collection<Chunk> chunks) {
        this.chunks = chunks;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this, asList("queryDesc", "querySplit", "hasNextQuerySplit"));
    }
}
