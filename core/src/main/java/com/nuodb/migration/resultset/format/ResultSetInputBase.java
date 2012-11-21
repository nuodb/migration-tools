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
package com.nuodb.migration.resultset.format;

import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.model.ColumnModel;
import com.nuodb.migration.jdbc.model.ColumnModelSet;
import com.nuodb.migration.jdbc.type.JdbcTypeDesc;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccess;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormat;
import com.nuodb.migration.resultset.format.xml.XmlEscape;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.util.List;

import static com.nuodb.migration.jdbc.model.ColumnModelFactory.createColumnModelSet;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ResultSetInputBase extends ResultSetFormatBase implements ResultSetInput {

    private transient final Log log = LogFactory.getLog(getClass());

    private Reader reader;
    private InputStream inputStream;
    private PreparedStatement preparedStatement;

    public Reader getReader() {
        return reader;
    }

    @Override
    public void setReader(Reader reader) {
        this.reader = reader;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
        initInput();
    }

    protected abstract void initInput();

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    @Override
    public void setPreparedStatement(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
        initColumnValueModelSet();
    }

    @Override
    protected void initColumnValueModelSet() {
        ColumnModelSet<ColumnValueModel> columnValueModelSet = getColumnValueModelSet();
        if (columnValueModelSet == null) {
            setColumnValueModelSet(createColumnValueModelSet());
        }
    }

    protected ColumnModelSet<ColumnValueModel> createColumnValueModelSet() {
        final List<ColumnValueModel> columnValues = Lists.newArrayList();
        int index = 0;
        for (ColumnModel column : getColumnModelSet()) {
            ColumnValueModel columnValue = createColumnValueModel(column, index++);
            visitColumnValueModel(columnValue);
            columnValues.add(columnValue);
        }
        return createColumnModelSet(columnValues);
    }

    protected ColumnValueModel createColumnValueModel(ColumnModel column, int index) {
        JdbcTypeDesc jdbcTypeDesc = new JdbcTypeDesc(column.getTypeCode(), column.getTypeName());
        JdbcTypeValueFormat columnValueFormat =
                getJdbcTypeValueFormatRegistry().getJdbcTypeValueFormat(jdbcTypeDesc);
        JdbcTypeValueAccess<Object> columnValueAccess =
                getJdbcTypeValueAccessProvider().getPreparedStatementAccess(getPreparedStatement(), column, index + 1);
        return new ColumnValueModelImpl(column, columnValueFormat, columnValueAccess, null);
    }

    @Override
    public final void readBegin() {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Read input %s", getClass().getName()));
        }
        doReadBegin();
    }

    protected abstract void doReadBegin();

    protected void readRow(String[] values) {
        ColumnModelSet<ColumnValueModel> columnValues = getColumnValueModelSet();
        for (int index = 0; index < values.length; index++) {
            ColumnValueModel columnValue = columnValues.get(index);
            String value = XmlEscape.INSTANCE.unescape(values[index]);
            columnValue.getValueFormat().setValue(
                    columnValue.getValueAccess(), value, columnValue.getValueAccessOptions());
        }
    }

    @Override
    public final void readEnd() {
        doReadEnd();
    }

    protected abstract void doReadEnd();
}
