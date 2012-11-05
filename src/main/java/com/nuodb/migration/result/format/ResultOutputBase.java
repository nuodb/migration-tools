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
package com.nuodb.migration.result.format;

import com.nuodb.migration.jdbc.metamodel.ColumnSetModel;
import com.nuodb.migration.jdbc.metamodel.ColumnSetModelFactory;
import com.nuodb.migration.jdbc.type.JdbcType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.OutputStream;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ResultOutputBase extends ResultFormatBase implements ResultOutput {

    private transient final Log log = LogFactory.getLog(getClass());

    private Writer writer;
    private OutputStream outputStream;
    private ResultSet resultSet;
    private ResultFormatModel resultFormatModel;

    @Override
    public final void initOutput() {
        doInitOutput();
    }

    protected abstract void doInitOutput();

    @Override
    public final void initModel() {
        doInitModel();
    }

    protected void doInitModel() {
        ColumnSetModel columnSetModel = getColumnSetModel();
        if (columnSetModel == null) {
            setColumnSetModel(createColumnSetModel());
        }
        ResultFormatModel resultFormatModel = getResultFormatModel();
        if (resultFormatModel == null) {
            setResultFormatModel(createResultFormatModel());
        }
    }

    protected ColumnSetModel createColumnSetModel() {
        ColumnSetModel columnSetModel;
        try {
            columnSetModel = ColumnSetModelFactory.createColumnSetModel(resultSet);
        } catch (SQLException exception) {
            throw new ResultOutputException(exception);
        }
        return columnSetModel;
    }

    /**
     * Try to dump data with resultSet.getString() value extraction, as it much faster <tt>JdbcType jdbcType =
     * JdbcCharType.INSTANCE;<tt/>
     */
    protected ResultFormatModel createResultFormatModel() {
        ColumnSetModel columnSetModel = getColumnSetModel();
        int columnCount = columnSetModel.getColumnCount();
        JdbcTypeValue[] columnValues = new JdbcTypeValue[columnCount];
        JdbcTypeFormat[] columnFormats = new JdbcTypeFormat[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int columnType = columnSetModel.getColumnType(i);
            JdbcType jdbcType = getJdbcTypeAccessor().getJdbcType(columnType);
            columnValues[i] = new JdbcTypeValueImpl(
                    getJdbcTypeAccessor().getJdbcTypeGet(jdbcType), resultSet, i + 1);
            columnFormats[i] = getJdbcTypeFormat(jdbcType);
        }
        return new ResultFormatModelImpl(columnValues, columnFormats, columnSetModel);
    }

    @Override
    public final void writeBegin() {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Write begin %s", getClass().getName()));
        }
        doWriteBegin();
    }

    protected abstract void doWriteBegin();

    @Override
    public final void writeRow() {
        doWriteRow(getColumnValues());
    }

    protected String[] getColumnValues() {
        ResultFormatModel model = getResultFormatModel();
        final String[] values = new String[model.getColumnCount()];
        for (int index = 0; index < model.getColumnCount(); index++) {
            JdbcTypeFormat format = model.getColumnFormat(index);
            JdbcTypeValue value = model.getColumnValue(index);
            values[index] = format.getValue(value);
        }
        return values;
    }

    protected abstract void doWriteRow(String[] values);

    @Override
    public void writeEnd() {
        if (log.isDebugEnabled()) {
            log.debug(String.format("End result output %s", getClass().getName()));
        }
        doWriteEnd();
    }

    protected abstract void doWriteEnd();

    @Override
    public final void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ResultFormatModel getResultFormatModel() {
        return resultFormatModel;
    }

    public void setResultFormatModel(ResultFormatModel resultFormatModel) {
        this.resultFormatModel = resultFormatModel;
    }

    public Writer getWriter() {
        return writer;
    }

    @Override
    public void setWriter(Writer writer) {
        this.writer = writer;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    @Override
    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }
}
