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
import com.nuodb.migration.jdbc.model.ColumnSetModel;
import com.nuodb.migration.jdbc.type.JdbcTypeDesc;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccess;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormat;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueSetModel;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueSetModelImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ResultSetInputBase extends ResultSetFormatBase implements ResultSetInput {

    private transient final Log log = LogFactory.getLog(getClass());

    private Reader reader;
    private InputStream inputStream;
    private PreparedStatement preparedStatement;
    private JdbcTypeValueSetModel jdbcTypeValueSetModel;

    public Reader getReader() {
        return reader;
    }

    @Override
    public void setReader(Reader reader) {
        this.reader = reader;
        doInitInput();
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
        doInitInput();
    }

    protected abstract void doInitInput();

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    @Override
    public void setPreparedStatement(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
        doInitJdbcTypeValueSetModel();
    }

    public JdbcTypeValueSetModel getJdbcTypeValueSetModel() {
        return jdbcTypeValueSetModel;
    }

    public void setJdbcTypeValueSetModel(JdbcTypeValueSetModel jdbcTypeValueSetModel) {
        this.jdbcTypeValueSetModel = jdbcTypeValueSetModel;
    }

    protected final void doInitJdbcTypeValueSetModel() {
        JdbcTypeValueSetModel jdbcTypeValueSetModel = getJdbcTypeValueSetModel();
        if (jdbcTypeValueSetModel == null) {
            setJdbcTypeValueSetModel(createJdbcTypeValueSetModel());
        }
    }

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
                    getStatementAccess(preparedStatement, index + 1, columnModel);
        }
        return new JdbcTypeValueSetModelImpl(jdbcTypeValueAccesses, jdbcTypeValueFormats, columnSetModel);
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
        JdbcTypeValueSetModel model = getJdbcTypeValueSetModel();
        for (int index = 0; index < model.getLength(); index++) {
            JdbcTypeValueAccess jdbcTypeValueAccess = model.getJdbcTypeValueAccess(index);
            JdbcTypeValueFormat jdbcTypeValueFormat = model.getJdbcTypeValueFormat(index);
            jdbcTypeValueFormat.setValue(jdbcTypeValueAccess, values[index]);
        }
    }

    @Override
    public final void readEnd() {
        doReadEnd();
    }

    protected abstract void doReadEnd();
}
