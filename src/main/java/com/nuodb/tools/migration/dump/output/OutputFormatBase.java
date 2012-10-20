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
import com.nuodb.tools.migration.jdbc.type.JdbcType;
import com.nuodb.tools.migration.jdbc.type.extract.JdbcTypeAcceptor;
import com.nuodb.tools.migration.jdbc.type.extract.JdbcTypeExtractor;

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
    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
        doSetAttributes();
    }

    protected void doSetAttributes() {
    }

    protected String getAttribute(String attribute) {
        return attributes.get(attribute);
    }

    protected String getAttribute(String attribute, String defaultValue) {
        String value = null;
        if (attributes != null) {
            value = attributes.get(attribute);
        }
        return value == null ? defaultValue : value;
    }

    protected Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public final void outputBegin(ResultSet resultSet) throws SQLException {
        resultSetMetaModel = new ResultSetMetaModel(resultSet);
        doOutputInit();
        doOutputBegin(resultSet);
    }

    protected void doOutputInit() {
    }

    protected void doOutputBegin(ResultSet resultSet) throws SQLException {
    }

    protected ResultSetMetaModel getResultSetMetaModel() {
        return resultSetMetaModel;
    }

    @SuppressWarnings("unchecked")
    protected String[] formatColumns(ResultSet resultSet) throws SQLException {
        final String[] columns = new String[resultSetMetaModel.getColumnCount()];
        final JdbcTypeAcceptor acceptor = new JdbcTypeAcceptor<Object>() {
            public int column;

            @Override
            public void accept(Object value, JdbcType jdbcType, int sqlType) throws SQLException {
                columns[column++] = formatColumn(value, column, jdbcType, sqlType);
            }
        };
        final JdbcTypeExtractor jdbcTypeExtractor = getJdbcTypeExtractor();
        for (int column = 0; column < resultSetMetaModel.getColumnCount(); column++) {
            jdbcTypeExtractor.<Object>extract(resultSet, column + 1, acceptor);
        }
        return columns;
    }

    @SuppressWarnings("unchecked")
    protected <T> String formatColumn(Object value, int column, JdbcType<T> jdbcType, int sqlType) {
        JdbcTypeFormatter<T> jdbcTypeFormatter = getJdbcTypeFormatter(sqlType);
        if (jdbcTypeFormatter == null) {
            jdbcTypeFormatter = getDefaultJdbcTypeFormatter();
        }
        return jdbcTypeFormatter.format((T) value, column, jdbcType, sqlType);
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

    @Override
    public void addJdbcTypeFormatter(JdbcType jdbcType, JdbcTypeFormatter jdbcTypeFormatter) {
        for (Integer sqlType : jdbcType.getSqlTypes()) {
            jdbcTypeFormatters.put(sqlType, jdbcTypeFormatter);
        }
    }

    @Override
    public JdbcTypeFormatter getJdbcTypeFormatter(int sqlType) {
        return jdbcTypeFormatters.get(sqlType);
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
