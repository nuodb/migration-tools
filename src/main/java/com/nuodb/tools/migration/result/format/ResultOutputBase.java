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
package com.nuodb.tools.migration.result.format;

import com.nuodb.tools.migration.jdbc.metamodel.ColumnSetModel;
import com.nuodb.tools.migration.jdbc.metamodel.ResultSetModel;
import com.nuodb.tools.migration.jdbc.type.JdbcType;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeAccessor;
import com.nuodb.tools.migration.result.format.jdbc.JdbcTypeFormat;
import com.nuodb.tools.migration.result.format.jdbc.JdbcTypeValue;
import com.nuodb.tools.migration.result.format.jdbc.JdbcTypeValueImpl;

import java.io.OutputStream;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ResultOutputBase extends ResultFormatBase implements ResultOutput {

    private Writer writer;
    private OutputStream outputStream;
    private JdbcTypeAccessor jdbcTypeAccessor;
    private ResultSet resultSet;
    private ColumnSetModel columnSetModel;
    private JdbcTypeValueAndFormat[] columnValuesAndFormats;

    class JdbcTypeValueAndFormat {
        private JdbcTypeValue jdbcTypeValue;
        private JdbcTypeFormat jdbcTypeFormat;

        public JdbcTypeValueAndFormat(JdbcTypeValue jdbcTypeValue, JdbcTypeFormat jdbcTypeFormat) {
            this.jdbcTypeValue = jdbcTypeValue;
            this.jdbcTypeFormat = jdbcTypeFormat;
        }

        public JdbcTypeValue getJdbcTypeValue() {
            return jdbcTypeValue;
        }

        public JdbcTypeFormat getJdbcTypeFormat() {
            return jdbcTypeFormat;
        }
    }

    @Override
    public final void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public final void outputStart() {
        doInitColumnModel();
        doOutputStart();
    }

    /**
     * Try to dump data with resultSet.getString() value extraction, as it much faster <tt>JdbcType jdbcType =
     * JdbcCharType.INSTANCE;<tt/>
     */
    protected void doInitColumnModel() {
        ColumnSetModel columnSetModel;
        try {
            columnSetModel = new ResultSetModel(resultSet);
        } catch (SQLException exception) {
            throw new ResultFormatException(exception);
        }
        int columnCount = columnSetModel.getColumnCount();
        JdbcTypeValueAndFormat[] columnValuesAndFormats = new JdbcTypeValueAndFormat[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int columnType = columnSetModel.getColumnType(i);
            JdbcType jdbcType = getJdbcTypeAccessor().getJdbcType(columnType);
            JdbcTypeFormat jdbcTypeFormat = getJdbcTypeFormat(jdbcType);
            JdbcTypeValueImpl jdbcTypeValue = new JdbcTypeValueImpl(
                    getJdbcTypeAccessor().getJdbcTypeGet(jdbcType), resultSet, i + 1);
            columnValuesAndFormats[i] = new JdbcTypeValueAndFormat(jdbcTypeValue, jdbcTypeFormat);
        }
        this.columnSetModel = columnSetModel;
        this.columnValuesAndFormats = columnValuesAndFormats;
    }

    protected ColumnSetModel getColumnSetModel() {
        return columnSetModel;
    }

    protected JdbcTypeValueAndFormat[] getColumnValuesAndFormats() {
        return columnValuesAndFormats;
    }

    protected abstract void doOutputStart();

    @Override
    public final void outputRow() {
        doOutputRow();
    }

    protected abstract void doOutputRow();

    @Override
    public void outputEnd() {
        doReleaseColumnModel();
        doOutputEnd();
    }

    protected void doReleaseColumnModel() {
        resultSet = null;
        columnSetModel = null;
        columnValuesAndFormats = null;
    }

    protected abstract void doOutputEnd();

    protected String[] getRowValues() {
        final String[] values = new String[columnSetModel.getColumnCount()];
        for (int i = 0; i < columnSetModel.getColumnCount(); i++) {
            JdbcTypeValueAndFormat columnValueAndFormat = columnValuesAndFormats[i];
            JdbcTypeFormat jdbcTypeFormat = columnValueAndFormat.getJdbcTypeFormat();
            JdbcTypeValue jdbcTypeValue = columnValueAndFormat.getJdbcTypeValue();
            values[i] = jdbcTypeFormat.format(jdbcTypeValue);
        }
        return values;
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

    public JdbcTypeAccessor getJdbcTypeAccessor() {
        return jdbcTypeAccessor;
    }

    @Override
    public void setJdbcTypeAccessor(JdbcTypeAccessor jdbcTypeAccessor) {
        this.jdbcTypeAccessor = jdbcTypeAccessor;
    }
}
