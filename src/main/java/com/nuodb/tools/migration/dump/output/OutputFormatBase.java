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
package com.nuodb.tools.migration.dump.output;

import com.nuodb.tools.migration.jdbc.metamodel.ResultSetMetaModel;
import com.nuodb.tools.migration.jdbc.type.extract.JdbcTypeAcceptor;
import com.nuodb.tools.migration.jdbc.type.extract.JdbcTypeExtractor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public abstract class OutputFormatBase implements OutputFormat {

    private Writer writer;
    private OutputStream outputStream;
    private Map<String, String> attributes;
    private JdbcTypeExtractor jdbcTypeExtractor;
    private Map<Integer, JdbcTypeFormatter> jdbcTypeFormatters = new HashMap<Integer, JdbcTypeFormatter>();
    private JdbcTypeFormatter defaultJdbcTypeFormatter = new JdbcTypeFormatterImpl();
    private ResultSetMetaModel resultSetMetaModel;

    @Override
    public final void outputBegin(ResultSet resultSet) throws IOException, SQLException {
        resultSetMetaModel = new ResultSetMetaModel(resultSet);
        doOutputInit();
        doOutputBegin(resultSet);
    }

    protected void doOutputInit() throws IOException {
    }

    protected void doOutputBegin(ResultSet resultSet) throws IOException, SQLException {
    }

    protected ResultSetMetaModel getResultSetMetaModel() {
        return resultSetMetaModel;
    }

    protected String[] formatColumns(ResultSet resultSet) throws IOException, SQLException {
        final String[] columns = new String[resultSetMetaModel.getColumnCount()];
        final JdbcTypeAcceptor acceptor = new JdbcTypeAcceptor() {
            public int column;

            @Override
            public void accept(Object value, int type) throws SQLException {
                columns[column++] = formatColumn(value, type);
            }
        };
        final JdbcTypeExtractor jdbcTypeExtractor = getJdbcTypeExtractor();
        for (int column = 0; column < resultSetMetaModel.getColumnCount(); column++) {
            jdbcTypeExtractor.extract(resultSet, column + 1, acceptor);
        }
        return columns;
    }

    protected String formatColumn(Object value, int type) {
        JdbcTypeFormatter jdbcTypeFormatter = getJdbcTypeFormatter(type);
        if (jdbcTypeFormatter == null) {
            jdbcTypeFormatter = getDefaultJdbcTypeFormatter();
        }
        return jdbcTypeFormatter.format(value, type);
    }

    @Override
    public void outputRow(ResultSet resultSet) throws IOException, SQLException {
        doOutputRow(resultSet);
    }

    protected abstract void doOutputRow(ResultSet resultSet) throws IOException, SQLException;

    @Override
    public final void outputEnd(ResultSet resultSet) throws IOException, SQLException {
        doOutputEnd(resultSet);
    }

    protected abstract void doOutputEnd(ResultSet resultSet) throws IOException, SQLException;

    @Override
    public void addJdbcTypeFormatter(int type, JdbcTypeFormatter jdbcTypeFormatter) {
        jdbcTypeFormatters.put(type, jdbcTypeFormatter);
    }

    @Override
    public JdbcTypeFormatter getJdbcTypeFormatter(int type) {
        return jdbcTypeFormatters.get(type);
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

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public JdbcTypeExtractor getJdbcTypeExtractor() {
        return jdbcTypeExtractor;
    }

    @Override
    public void setJdbcTypeExtractor(JdbcTypeExtractor jdbcTypeExtractor) {
        this.jdbcTypeExtractor = jdbcTypeExtractor;
    }

    public JdbcTypeFormatter getDefaultJdbcTypeFormatter() {
        return defaultJdbcTypeFormatter;
    }

    @Override
    public void setDefaultJdbcTypeFormatter(JdbcTypeFormatter defaultJdbcTypeFormatter) {
        this.defaultJdbcTypeFormatter = defaultJdbcTypeFormatter;
    }
}
