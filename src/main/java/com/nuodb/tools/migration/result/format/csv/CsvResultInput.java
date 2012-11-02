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
package com.nuodb.tools.migration.result.format.csv;

import com.google.common.collect.Lists;
import com.nuodb.tools.migration.result.format.ColumnDataModel;
import com.nuodb.tools.migration.result.format.ColumnDataModelImpl;
import com.nuodb.tools.migration.result.format.ResultFormatException;
import com.nuodb.tools.migration.result.format.ResultInputBase;
import com.nuodb.tools.migration.jdbc.metamodel.ColumnSetModel;
import com.nuodb.tools.migration.jdbc.type.JdbcTypeAccessor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.Quote;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static java.lang.Boolean.parseBoolean;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class CsvResultInput extends ResultInputBase implements CsvResultFormat {

    private JdbcTypeAccessor jdbcTypeAccess;
    /**
     * The symbol used for value separation, must not be a line break character.
     */
    private Character delimiter;
    /**
     * Indicates whether quotation should be used.
     */
    private boolean quoting;
    /**
     * The symbol used as value encapsulation marker.
     */
    private Character quote;
    /**
     * The symbol used to escape special characters in values.
     */
    private Character escape;
    /**
     * The record separator to use for withConnection.
     */
    private String lineSeparator;

    private CSVParser parser;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected void doSetAttributes() {
        String delimiterValue = getAttribute(ATTRIBUTE_DELIMITER);
        if (isEmpty(delimiterValue)) {
            delimiter = DELIMITER;
        } else {
            delimiter = delimiterValue.charAt(0);
        }

        String quotingValue = getAttribute(ATTRIBUTE_QUOTING);
        if (isEmpty(quotingValue)) {
            quoting = QUOTING;
        } else {
            quoting = parseBoolean(quotingValue);
        }
        String quoteValue = getAttribute(ATTRIBUTE_QUOTE);
        if (isEmpty(quoteValue)) {
            quote = QUOTE;
        } else {
            quote = quoteValue.charAt(0);
        }

        String escapeValue = getAttribute(ATTRIBUTE_ESCAPE);
        if (isEmpty(escapeValue)) {
            escape = ESCAPE;
        } else {
            escape = escapeValue.charAt(0);
        }
        lineSeparator = getAttribute(ATTRIBUTE_LINE_SEPARATOR, LINE_SEPARATOR);
    }

    public void init() {
        CSVFormat format = new CSVFormat(delimiter);
        if (quoting && quote != null) {
            format = format.withQuotePolicy(Quote.ALL);
            format = format.withQuoteChar(quote);
        }
        format = format.withRecordSeparator(lineSeparator);
        format = format.withEscape(escape);

        try {
            if (getReader() != null) {
                parser = new CSVParser(getReader(), format);
            } else if (getInputStream() != null) {
                parser = new CSVParser(new InputStreamReader(getInputStream()), format);
            }
        } catch (IOException exception) {
            throw new ResultFormatException(exception);
        }
    }

    public ColumnDataModel read() {
        Iterator<CSVRecord> iterator = parser.iterator();
        List<String> columns = Lists.newArrayList();
        if (iterator.hasNext()) {
            for (String column : iterator.next()) {
                columns.add(column);
            }
        }
        return new ColumnDataModelImpl(columns);
    }

    public boolean bind(final PreparedStatement statement, ColumnSetModel model) throws SQLException {
        final Connection connection = statement.getConnection();
        Iterator<CSVRecord> iterator = parser.iterator();
        if (iterator.hasNext()) {
            final CSVRecord record = iterator.next();
            for (int column = 0; column < model.getColumnCount(); column++) {
                final String value = record.get(column);
//                jdbcTypeAccess.bind(statement, column + 1, model.getColumnType(column),
//                        new JdbcTypeSet<Object>() {
//                            @Override
//                            public Object provide(PreparedStatement preparedStatement, int column,
//                                                  JdbcType<Object> jdbcType) {
//                                JdbcTypeFormat format = getJdbcTypeFormat(jdbcType.getTypeCode());
//                                if (format == null) {
//                                    format = getDefaultJdbcTypeFormat();
//                                }
//                                return format.parse(value, column, jdbcType, connection);
//                            }
//                        });
            }
            return true;
        } else {
            return false;
        }

    }

    public JdbcTypeAccessor getJdbcTypeAccess() {
        return jdbcTypeAccess;
    }

    public void setJdbcTypeAccess(JdbcTypeAccessor jdbcTypeAccess) {
        this.jdbcTypeAccess = jdbcTypeAccess;
    }
}
