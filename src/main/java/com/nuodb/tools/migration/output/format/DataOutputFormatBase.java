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
package com.nuodb.tools.migration.output.format;

import com.nuodb.tools.migration.output.format.jdbc.JdbcTypeFormat;
import com.nuodb.tools.migration.output.format.jdbc.JdbcTypeValueGet;
import com.nuodb.tools.migration.jdbc.metamodel.ResultSetModel;
import com.nuodb.tools.migration.jdbc.type.JdbcType;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeAccessor;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeGet;

import java.io.OutputStream;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class DataOutputFormatBase extends DataFormatBase implements DataOutputFormat {

    private Writer writer;
    private OutputStream outputStream;
    private JdbcTypeAccessor jdbcTypeAccessor;
    private ResultSetModel resultSetModel;

    @Override
    public final void outputBegin(ResultSet resultSet) throws SQLException {
        resultSetModel = new ResultSetModel(resultSet);
        doOutputInit();
        doOutputBegin(resultSet);
    }

    protected void doOutputInit() {
    }

    protected void doOutputBegin(ResultSet resultSet) throws SQLException {
    }

    protected ResultSetModel getResultSetModel() {
        return resultSetModel;
    }

    protected String[] formatColumns(ResultSet resultSet) throws SQLException {
        final String[] columns = new String[resultSetModel.getColumnCount()];
        for (int column = 0; column < resultSetModel.getColumnCount(); column++) {
            int typeCode = resultSetModel.getColumnType(column);
            JdbcTypeGet jdbcTypeGet = getJdbcTypeAccessor().getJdbcTypeGet(typeCode);
            columns[column] = formatColumn(jdbcTypeGet, resultSet, column + 1);
        }
        return columns;
    }

    protected String formatColumn(JdbcTypeGet jdbcTypeGet, ResultSet resultSet, int column) throws SQLException {
        JdbcType jdbcType = jdbcTypeGet.getJdbcType();
        JdbcTypeFormat jdbcTypeFormat = getJdbcTypeFormat(jdbcType.getTypeCode());
        if (jdbcTypeFormat == null) {
            jdbcTypeFormat = getDefaultJdbcTypeFormat();
        }
        return jdbcTypeFormat.format(new JdbcTypeValueGet(jdbcTypeGet, resultSet, column));
    }

    @Override
    public void outputRow(ResultSet resultSet) throws SQLException {
        doOutputRow(resultSet);
    }

    protected abstract void doOutputRow(ResultSet resultSet) throws SQLException;

    @Override
    public final void outputEnd(ResultSet resultSet) throws SQLException {
        doOutputEnd(resultSet);
    }

    protected abstract void doOutputEnd(ResultSet resultSet) throws SQLException;

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
