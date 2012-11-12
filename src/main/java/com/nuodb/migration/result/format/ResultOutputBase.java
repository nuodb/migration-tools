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

import com.nuodb.migration.jdbc.metamodel.ColumnModelFactory;
import com.nuodb.migration.jdbc.metamodel.ColumnModelSet;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccessor;
import com.nuodb.migration.result.format.jdbc.JdbcTypeValueModelSet;
import com.nuodb.migration.result.format.jdbc.JdbcTypeValueFormat;
import com.nuodb.migration.result.format.jdbc.JdbcTypeValueModelSetImpl;
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
    private JdbcTypeValueModelSet jdbcTypeValueModelSet;

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
        ColumnModelSet columnModelSet = getColumnModelSet();
        if (columnModelSet == null) {
            setColumnModelSet(createColumnSetModel());
        }
        JdbcTypeValueModelSet jdbcTypeValueModelSet = getJdbcTypeValueModelSet();
        if (jdbcTypeValueModelSet == null) {
            setJdbcTypeValueModelSet(createJdbcTypeValueModelSet());
        }
    }

    protected ColumnModelSet createColumnSetModel() {
        ColumnModelSet columnModelSet;
        try {
            columnModelSet = ColumnModelFactory.createColumnModelSet(resultSet);
        } catch (SQLException exception) {
            throw new ResultOutputException(exception);
        }
        return columnModelSet;
    }

    /**
     * Try to dump data with resultSet.getString() value extraction, as it much faster <tt>JdbcType jdbcType =
     * JdbcCharType.INSTANCE;<tt/>
     */
    protected JdbcTypeValueModelSet createJdbcTypeValueModelSet() {
        final ColumnModelSet columnModelSet = getColumnModelSet();
        final int valueCount = columnModelSet.getLength();
        JdbcTypeValueAccessor[] accessors = new JdbcTypeValueAccessor[valueCount];
        JdbcTypeValueFormat[] formats = new JdbcTypeValueFormat[valueCount];
        for (int index = 0; index < valueCount; index++) {
            int typeCode = columnModelSet.getTypeCode(index);
            accessors[index] =  getJdbcTypeValueAccess().createResultSetAccessor(
                    resultSet, index + 1, columnModelSet.item(index));
            formats[index] = getJdbcTypeValueFormat(typeCode);
        }
        return new JdbcTypeValueModelSetImpl(accessors, formats, columnModelSet);
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
        JdbcTypeValueModelSet model = getJdbcTypeValueModelSet();
        final String[] values = new String[model.getLength()];
        for (int index = 0; index < model.getLength(); index++) {
            JdbcTypeValueFormat format = model.getJdbcTypeValueFormat(index);
            JdbcTypeValueAccessor accessor = model.getJdbcTypeValueAccessor(index);
            values[index] = format.getValue(accessor);
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

    public JdbcTypeValueModelSet getJdbcTypeValueModelSet() {
        return jdbcTypeValueModelSet;
    }

    public void setJdbcTypeValueModelSet(JdbcTypeValueModelSet jdbcTypeValueModelSet) {
        this.jdbcTypeValueModelSet = jdbcTypeValueModelSet;
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
