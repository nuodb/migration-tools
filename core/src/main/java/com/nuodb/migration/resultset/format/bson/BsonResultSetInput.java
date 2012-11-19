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
package com.nuodb.migration.resultset.format.bson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.model.ColumnModelFactory;
import com.nuodb.migration.jdbc.model.ColumnSetModel;
import com.nuodb.migration.jdbc.type.jdbc2.JdbcCharType;
import com.nuodb.migration.resultset.format.ResultSetInputBase;
import com.nuodb.migration.resultset.format.ResultSetInputException;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.*;
import static de.undercouch.bson4jackson.BsonGenerator.Feature.ENABLE_STREAMING;

/**
 * @author Sergey Bushik
 */
public class BsonResultSetInput extends ResultSetInputBase implements BsonAttributes {

    private JsonParser parser;
    private Iterator<String[]> iterator;

    @Override
    public String getFormatType() {
        return FORMAT_TYPE;
    }

    @Override
    protected void doInitInput() {
        BsonFactory generatorFactory = new BsonFactory();
        generatorFactory.enable(ENABLE_STREAMING);
        try {
            if (getReader() != null) {
                parser = generatorFactory.createJsonParser(getReader());
            } else if (getInputStream() != null) {
                parser = generatorFactory.createJsonParser(getInputStream());
            }
        } catch (IOException exception) {
            throw new ResultSetInputException(exception);
        }
        iterator = createRowsIterator();
    }

    protected Iterator<String[]> createRowsIterator() {
        return new BsonIterator();
    }

    @Override
    protected void doReadBegin() {
        ColumnSetModel columnSetModel = null;
        try {
            if (isNextToken(START_OBJECT) && isNextField(COLUMNS_FIELD) && isNextToken(START_OBJECT)) {
                List<String> columns = Lists.newArrayList();
                while (isNextField(COLUMN_FIELD) && isNextToken(VALUE_STRING)) {
                    columns.add(parser.getText());
                }
                parser.nextToken();
                int[] columnTypes = new int[columns.size()];
                Arrays.fill(columnTypes, JdbcCharType.INSTANCE.getTypeDesc().getTypeCode());
                columnSetModel = ColumnModelFactory.createColumnSetModel(columns.toArray(new String[columns.size()]),
                        columnTypes);
            }
            parser.nextToken();
            parser.nextToken();
        } catch (IOException exception) {
            throw new ResultSetInputException(exception);
        }
        setColumnSetModel(columnSetModel);
    }

    protected boolean isCurrentToken(JsonToken token) {
        return token.equals(parser.getCurrentToken());
    }

    protected boolean isNextToken(JsonToken token) throws IOException {
        return token.equals(parser.nextToken());
    }

    protected boolean isNextField(String field) throws IOException {
        return isNextToken(JsonToken.FIELD_NAME) && field.equals(parser.getText());
    }

    @Override
    public boolean readNextRow() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public void readRow() {
        readRow(iterator.next());
    }

    protected String[] doReadRow() {
        String[] values = null;
        try {
            if (isCurrentToken(START_ARRAY)) {
                values = new String[getColumnSetModel().getLength()];
                int column = 0;
                parser.nextToken();
                while (isCurrentToken(VALUE_NULL) || isCurrentToken(VALUE_STRING)) {
                    values[column++] = parser.getText();
                    parser.nextToken();
                }
                parser.nextToken();
            }
        } catch (IOException exception) {
            throw new ResultSetInputException(exception);
        }
        return values;
    }

    @Override
    protected void doReadEnd() {
        try {
            parser.close();
        } catch (IOException exception) {
            throw new ResultSetInputException(exception);
        }
    }


    class BsonIterator implements Iterator<String[]> {

        private String[] current;

        @Override
        public boolean hasNext() {
            if (current == null) {
                current = doReadRow();
            }
            return current != null;
        }

        @Override
        public String[] next() {
            String[] next = current;
            current = null;
            if (next == null) {
                // hasNext() wasn't called before
                next = doReadRow();
                if (next == null) {
                    throw new ResultSetInputException("No more rows available");
                }
            }
            return next;
        }

        @Override
        public void remove() {
            throw new ResultSetInputException("Removal is unsupported operation");
        }
    }
}
