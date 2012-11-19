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
package com.nuodb.migration.resultset.format.csv;

import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.model.ColumnModelFactory;
import com.nuodb.migration.jdbc.model.ColumnSetModel;
import com.nuodb.migration.jdbc.type.jdbc2.JdbcCharType;
import com.nuodb.migration.resultset.format.ResultSetInputBase;
import com.nuodb.migration.resultset.format.ResultSetInputException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
public class CsvResultSetInput extends ResultSetInputBase implements CsvAttributes {

    private CSVParser parser;
    private String doubleQuote;
    private Iterator<CSVRecord> iterator;

    @Override
    public String getFormatType() {
        return FORMAT_TYPE;
    }

    @Override
    protected void doInitInput() {
        CsvFormatBuilder builder = new CsvFormatBuilder(this);
        CSVFormat format = builder.build();
        Character quote = builder.getQuote();
        doubleQuote = valueOf(quote) + valueOf(quote);
        try {
            if (getReader() != null) {
                parser = new CSVParser(getReader(), format);
            } else if (getInputStream() != null) {
                parser = new CSVParser(new InputStreamReader(getInputStream()), format);
            }
        } catch (IOException exception) {
            throw new ResultSetInputException(exception);
        }
        iterator = parser.iterator();
    }

    @Override
    protected void doReadBegin() {
        ColumnSetModel columnSetModel = null;
        if (iterator.hasNext()) {
            List<String> columns = Lists.newArrayList();
            for (String column : iterator.next()) {
                columns.add(column);
            }
            int[] columnTypes = new int[columns.size()];
            Arrays.fill(columnTypes, JdbcCharType.INSTANCE.getTypeDesc().getTypeCode());
            columnSetModel = ColumnModelFactory.createColumnSetModel(columns.toArray(new String[columns.size()]),
                    columnTypes);
        }
        setColumnSetModel(columnSetModel);
    }

    @Override
    public boolean hasNextRow() {
        return iterator.hasNext();
    }

    @Override
    public void readRow() {
        String[] values = new String[getColumnSetModel().getLength()];
        Iterator<String> iterator = this.iterator.next().iterator();
        int column = 0;
        while (iterator.hasNext()) {
            String value = iterator.next();
            if (doubleQuote.equals(value)) {
                value = StringUtils.EMPTY;
            }
            values[column++] = value;

        }
        readRow(values);
    }

    @Override
    protected void doReadEnd() {
    }
}
