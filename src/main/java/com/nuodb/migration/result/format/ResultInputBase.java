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
import com.nuodb.migration.jdbc.type.JdbcType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ResultInputBase extends ResultFormatBase implements ResultInput {

    private transient final Log log = LogFactory.getLog(getClass());

    private Reader reader;
    private InputStream inputStream;
    private PreparedStatement preparedStatement;
    private ResultFormatModel resultFormatModel;

    @Override
    public void initInput() {
        doInitInput();
    }

    protected abstract void doInitInput();

    @Override
    public final void initModel() {
        doInitModel();
    }

    protected void doInitModel() {
        ResultFormatModel resultFormatModel = getResultFormatModel();
        if (resultFormatModel == null) {
            setResultFormatModel(createResultFormatModel());
        }
    }

    protected ResultFormatModel createResultFormatModel() {
        ColumnSetModel columnSetModel = getColumnSetModel();
        int columnCount = columnSetModel.getColumnCount();
        JdbcTypeValue[] columnValues = new JdbcTypeValue[columnCount];
        JdbcTypeFormat[] columnFormats = new JdbcTypeFormat[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int columnType = columnSetModel.getColumnType(i);
            JdbcType jdbcType = getJdbcTypeAccessor().getJdbcType(columnType);
            columnValues[i] = new JdbcTypeValueImpl(
                    getJdbcTypeAccessor().getJdbcTypeSet(jdbcType), preparedStatement, i + 1);
            columnFormats[i] = getJdbcTypeFormat(jdbcType);
        }
        return new ResultFormatModelImpl(columnValues, columnFormats, columnSetModel);
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
        ResultFormatModel model = getResultFormatModel();
        for (int index = 0; index < model.getColumnCount(); index++) {
            JdbcTypeValue value = model.getColumnValue(index);
            JdbcTypeFormat format = model.getColumnFormat(index);
            format.setValue(value, values[index]);
        }
    }

    @Override
    public final void readEnd() {
        doReadEnd();
    }

    protected abstract void doReadEnd();

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
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    @Override
    public void setPreparedStatement(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
    }

    public ResultFormatModel getResultFormatModel() {
        return resultFormatModel;
    }

    public void setResultFormatModel(ResultFormatModel resultFormatModel) {
        this.resultFormatModel = resultFormatModel;
    }
}
