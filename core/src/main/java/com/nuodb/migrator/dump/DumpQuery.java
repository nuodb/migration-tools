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

import com.google.common.collect.Lists;
import com.nuodb.migrator.backup.catalog.Chunk;
import com.nuodb.migrator.backup.catalog.QueryRowSet;
import com.nuodb.migrator.backup.catalog.RowSet;
import com.nuodb.migrator.backup.format.OutputFormat;
import com.nuodb.migrator.backup.format.value.ValueHandleList;
import com.nuodb.migrator.jdbc.JdbcUtils;
import com.nuodb.migrator.jdbc.metadata.Table;
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
class DumpQuery implements DumpTask {

    private static final String QUERY = "query";

    private final DumpQueryMonitor dumpQueryMonitor;
    private final QueryHandle queryHandle;
    private final QuerySplit querySplit;
    private final RowSet rowSet;
    private final boolean hasNextQuerySplit;

    private DumpContext dumpContext;
    private ResultSet resultSet;
    private Statement statement;
    private ValueHandleList valueHandleList;
    private OutputFormat outputFormat;
    private Collection<Chunk> chunks;

    public DumpQuery(DumpQueryMonitor dumpQueryMonitor, QueryHandle queryHandle, QuerySplit querySplit,
                     RowSet rowSet, boolean hasNextQuerySplit) {
        this.dumpQueryMonitor = dumpQueryMonitor;
        this.queryHandle = queryHandle;
        this.querySplit = querySplit;
        this.rowSet = rowSet;
        this.hasNextQuerySplit = hasNextQuerySplit;
    }

    @Override
    public void init(DumpContext dumpContext) throws Exception {
        setDumpContext(dumpContext);
        setChunks(Lists.<Chunk>newArrayList());
        setResultSet(getQuerySplit().getResultSet());
        setStatement(getResultSet().getStatement());
        setValueHandleList(createValueHandleList());
        setOutputFormat(createOutputFormat());
        initRowSetName();
    }

    @Override
    public void execute() throws Exception {
        final DumpQueryMonitor dumpQueryMonitor = getDumpQueryMonitor();
        dumpQueryMonitor.executeStart(this);

        final ResultSet resultSet = getResultSet();
        final OutputFormat outputFormat = getOutputFormat();

        Chunk chunk = null;
        while (dumpQueryMonitor.canWrite(this) && resultSet.next()) {
            if (chunk == null) {
                writeStart(chunk = addChunk());
            }
            if (!outputFormat.canWriteValues()) {
                writeEnd(chunk);
                writeStart(chunk = addChunk());
            }
            outputFormat.writeValues();
            dumpQueryMonitor.writeValues(this, chunk);
        }
        if (chunk != null) {
            writeEnd(chunk);
        }
        dumpQueryMonitor.executeEnd(this);
    }

    @Override
    public void close() {
        JdbcUtils.close(getResultSet());
        JdbcUtils.close(getStatement());
    }

    protected ValueHandleList createValueHandleList() throws SQLException {
        return newBuilder(getResultSet()).
                withColumns(createColumnList(getResultSet())).
                withDialect(getDumpContext().getDialect()).
                withTimeZone(getDumpContext().getTimeZone()).
                withValueFormatRegistry(getDumpContext().getValueFormatRegistry()).build();
    }

    protected OutputFormat createOutputFormat() {
        OutputFormat outputFormat = getDumpContext().getFormatFactory().createOutputFormat(
                getDumpContext().getFormat(), getDumpContext().getFormatAttributes());
        outputFormat.setValueHandleList(getValueHandleList());
        return outputFormat;
    }

    protected void writeStart(Chunk chunk) throws Exception {
        final OutputFormat outputFormat = getOutputFormat();
        outputFormat.setOutputStream(getDumpContext().getCatalogManager().openOutputStream(chunk.getName()));
        outputFormat.open();
        outputFormat.writeStart();

        getDumpQueryMonitor().writeStart(this, chunk);
    }

    protected void writeEnd(Chunk chunk) throws Exception {
        final OutputFormat outputFormat = getOutputFormat();
        outputFormat.writeEnd();
        outputFormat.close();
        getDumpQueryMonitor().writeEnd(this, chunk);
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

    protected void initRowSetName() {
        if (getRowSet().getName() == null) {
            getRowSet().setName(getRowSetName());
        }
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
        parts.add(getDumpContext().getFormat());
        return lowerCase(join(parts, "."));
    }

    protected String getRowSetName() {
        String rowSetName;
        QueryHandle queryHandle = getQueryHandle();
        if (queryHandle instanceof TableQueryHandle) {
            Table table = ((TableQueryHandle) queryHandle).getTable();
            rowSetName = table.getQualifiedName(null);
        } else {
            int rowSetIndex = indexOf(filter(getRowSet().getCatalog().getRowSets(),
                    instanceOf(QueryRowSet.class)), equalTo(getRowSet()));
            rowSetName = join(asList(QUERY, rowSetIndex + 1), "-");
        }
        return rowSetName;
    }

    public DumpQueryMonitor getDumpQueryMonitor() {
        return dumpQueryMonitor;
    }

    public QueryHandle getQueryHandle() {
        return queryHandle;
    }

    public QuerySplit getQuerySplit() {
        return querySplit;
    }

    public boolean isHasNextQuerySplit() {
        return hasNextQuerySplit;
    }

    public RowSet getRowSet() {
        return rowSet;
    }

    public DumpContext getDumpContext() {
        return dumpContext;
    }

    public void setDumpContext(DumpContext dumpContext) {
        this.dumpContext = dumpContext;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
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
