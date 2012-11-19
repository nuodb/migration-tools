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

import com.nuodb.migration.jdbc.model.ColumnModel;
import com.nuodb.migration.jdbc.model.ColumnModelFactory;
import com.nuodb.migration.jdbc.model.ColumnSetModel;
import com.nuodb.migration.jdbc.type.JdbcTypeDesc;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccess;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormat;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueSetModel;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueSetModelImpl;
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
public abstract class ResultSetOutputBase extends ResultSetFormatBase implements ResultSetOutput {

    private transient final Log log = LogFactory.getLog(getClass());

    private Writer writer;
    private OutputStream outputStream;
    private ResultSet resultSet;
    private JdbcTypeValueSetModel jdbcTypeValueSetModel;

    public Writer getWriter() {
        return writer;
    }

    @Override
    public void setWriter(Writer writer) {
        this.writer = writer;
        doInitOutput();
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    @Override
    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
        doInitOutput();
    }

    protected abstract void doInitOutput();

    @Override
    public final void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
        initJdbcTypeValueSetModel();
    }

    public JdbcTypeValueSetModel getJdbcTypeValueSetModel() {
        return jdbcTypeValueSetModel;
    }

    public void setJdbcTypeValueSetModel(JdbcTypeValueSetModel jdbcTypeValueSetModel) {
        this.jdbcTypeValueSetModel = jdbcTypeValueSetModel;
    }

    protected void initJdbcTypeValueSetModel() {
        ColumnSetModel columnSetModel = getColumnSetModel();
        if (columnSetModel == null) {
            setColumnSetModel(createColumnSetModel());
        }
        JdbcTypeValueSetModel jdbcTypeValueModelSet = getJdbcTypeValueSetModel();
        if (jdbcTypeValueModelSet == null) {
            setJdbcTypeValueSetModel(createJdbcTypeValueSetModel());
        }
    }

    protected ColumnSetModel createColumnSetModel() {
        ColumnSetModel columnSetModel;
        try {
            columnSetModel = ColumnModelFactory.createColumnSetModel(resultSet);
        } catch (SQLException exception) {
            throw new ResultSetOutputException(exception);
        }
        return columnSetModel;
    }

    /**
     * Try to dump data with resultSet.getString() value extraction, as it much faster <tt>JdbcType jdbcType =
     * JdbcCharType.INSTANCE;<tt/>
     */
    protected JdbcTypeValueSetModel createJdbcTypeValueSetModel() {
        final ColumnSetModel columnSetModel = getColumnSetModel();
        final int valueCount = columnSetModel.getLength();
        JdbcTypeValueAccess[] jdbcTypeValueAccesses = new JdbcTypeValueAccess[valueCount];
        JdbcTypeValueFormat[] jdbcTypeValueFormats = new JdbcTypeValueFormat[valueCount];
        for (int index = 0; index < valueCount; index++) {
            ColumnModel columnModel = columnSetModel.item(index);
            JdbcTypeDesc jdbcTypeDesc = new JdbcTypeDesc(columnModel.getTypeCode(), columnModel.getTypeName());
            jdbcTypeValueFormats[index] = getJdbcTypeValueFormatRegistry().
                    getJdbcTypeValueFormat(jdbcTypeDesc);
            jdbcTypeValueAccesses[index] = getJdbcTypeValueAccessProvider().
                    getResultSetAccess(resultSet, index + 1, columnModel);

        }
        return new JdbcTypeValueSetModelImpl(jdbcTypeValueAccesses, jdbcTypeValueFormats, columnSetModel);
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
        JdbcTypeValueSetModel jdbcTypeValueSetModel = getJdbcTypeValueSetModel();
        final String[] columnValues = new String[jdbcTypeValueSetModel.getLength()];
        for (int index = 0; index < jdbcTypeValueSetModel.getLength(); index++) {
            JdbcTypeValueFormat jdbcTypeValueFormat = jdbcTypeValueSetModel.getJdbcTypeValueFormat(index);
            JdbcTypeValueAccess jdbcTypeValueAccess = jdbcTypeValueSetModel.getJdbcTypeValueAccess(index);
            columnValues[index] = jdbcTypeValueFormat.getValue(jdbcTypeValueAccess);
        }
        return columnValues;
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
}
