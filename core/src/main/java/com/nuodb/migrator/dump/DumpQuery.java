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
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.session.WorkBase;
import com.nuodb.migrator.jdbc.split.QuerySplit;
import com.nuodb.migrator.utils.ObjectUtils;

import java.sql.Connection;
import java.sql.ResultSet;
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
public class DumpQuery extends WorkBase {

    private static final String QUERY = "query";

    private final DumpWriterContext dumpWriterContext;
    private final DumpQueryObserver dumpQueryObserver;
    private final QueryInfo queryInfo;
    private final QuerySplit querySplit;
    private final boolean hasNextQuerySplit;
    private final RowSet rowSet;

    private ResultSet resultSet;
    private ValueHandleList valueHandleList;
    private OutputFormat outputFormat;
    private Collection<Chunk> chunks;

    public DumpQuery(DumpWriterContext dumpWriterContext, DumpQueryObserver dumpQueryObserver,
                     QueryInfo queryInfo, QuerySplit querySplit, boolean hasNextQuerySplit, RowSet rowSet) {
        this.dumpWriterContext = dumpWriterContext;
        this.dumpQueryObserver = dumpQueryObserver;
        this.queryInfo = queryInfo;
        this.querySplit = querySplit;
        this.hasNextQuerySplit = hasNextQuerySplit;
        this.rowSet = rowSet;
    }

    @Override
    public void init() throws Exception {
        DumpWriterContext dumpWriterContext = getDumpWriterContext();

        Connection connection = getSession().getConnection();
        Dialect dialect = getSession().getDialect();
        setResultSet(getQuerySplit().getResultSet(connection));


        setValueHandleList(newBuilder(getResultSet()).
                withDialect(dialect).
                withColumns(createColumnList(getResultSet())).
                withTimeZone(dumpWriterContext.getTimeZone()).
                withValueFormatRegistry(dumpWriterContext.getValueFormatRegistry()).build());

        OutputFormat outputFormat = dumpWriterContext.getFormatFactory().createOutputFormat(
                dumpWriterContext.getFormat(), dumpWriterContext.getFormatAttributes());
        outputFormat.setValueHandleList(getValueHandleList());

        setOutputFormat(outputFormat);

        setChunks(Lists.<Chunk>newArrayList());
        if (getRowSet().getName() == null) {
            getRowSet().setName(getRowSetName());
        }
    }

    @Override
    public void execute() throws Exception {
        DumpQueryObserver dumpQueryObserver = getDumpQueryObserver();
        dumpQueryObserver.writeStart(this);

        ResultSet resultSet = getResultSet();
        OutputFormat outputFormat = getOutputFormat();

        Chunk chunk = null;
        while (dumpQueryObserver.canWrite(this) && resultSet.next()) {
            if (chunk == null) {
                writeStart(chunk = addChunk());
            }
            if (!outputFormat.canWriteValues()) {
                writeEnd(chunk);
                writeStart(chunk = addChunk());
            }
            outputFormat.writeValues();
            dumpQueryObserver.writeValues(this, chunk);
        }
        if (chunk != null) {
            writeEnd(chunk);
        }
        dumpQueryObserver.writeEnd(this);
    }

    @Override
    public void close() throws Exception {
        JdbcUtils.close(resultSet);
    }

    protected void writeStart(Chunk chunk) throws Exception {
        outputFormat.setOutputStream(dumpWriterContext.getCatalogManager().openOutputStream(chunk.getName()));
        outputFormat.open();
        outputFormat.writeStart();

        dumpQueryObserver.writeStart(this, chunk);
    }

    protected void writeEnd(Chunk chunk) throws Exception {
        outputFormat.writeEnd();
        outputFormat.close();
        dumpQueryObserver.writeEnd(this, chunk);
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
        parts.add(dumpWriterContext.getFormat());
        return lowerCase(join(parts, "."));
    }

    protected String getRowSetName() {
        String rowSetName;
        if (queryInfo instanceof TableQueryInfo) {
            Table table = ((TableQueryInfo) queryInfo).getTable();
            rowSetName = table.getQualifiedName(null);
        } else {
            int rowSetIndex = indexOf(filter(rowSet.getCatalog().getRowSets(),
                    instanceOf(QueryRowSet.class)), equalTo(getRowSet()));
            rowSetName = join(asList(QUERY, rowSetIndex + 1), "-");
        }
        return rowSetName;
    }

    public DumpWriterContext getDumpWriterContext() {
        return dumpWriterContext;
    }

    public DumpQueryObserver getDumpQueryObserver() {
        return dumpQueryObserver;
    }

    public QueryInfo getQueryInfo() {
        return queryInfo;
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
