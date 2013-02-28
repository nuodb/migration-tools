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
package com.nuodb.migrator.resultset.format.bson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.nuodb.migrator.jdbc.model.ValueModelList;
import com.nuodb.migrator.resultset.format.FormatInputBase;
import com.nuodb.migrator.resultset.format.FormatInputException;
import com.nuodb.migrator.resultset.format.value.SimpleValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueVariant;
import com.nuodb.migrator.resultset.format.value.ValueVariantType;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.IOException;
import java.util.Iterator;

import static com.fasterxml.jackson.core.JsonToken.*;
import static com.nuodb.migrator.jdbc.model.ValueModelFactory.createValueModelList;
import static com.nuodb.migrator.resultset.format.value.ValueVariantType.STRING;
import static com.nuodb.migrator.resultset.format.value.ValueVariantType.fromAlias;
import static com.nuodb.migrator.resultset.format.value.ValueVariantUtils.fill;
import static com.nuodb.migrator.resultset.format.value.ValueVariants.binary;
import static com.nuodb.migrator.resultset.format.value.ValueVariants.string;
import static de.undercouch.bson4jackson.BsonGenerator.Feature.ENABLE_STREAMING;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;

/**
 * @author Sergey Bushik
 */
public class BsonInputFormat extends FormatInputBase implements BsonAttributes {

    private JsonParser reader;
    private Iterator<ValueVariant[]> iterator;

    @Override
    public String getType() {
        return FORMAT_TYPE;
    }

    @Override
    public void initInput() {
        BsonFactory factory = new BsonFactory();
        factory.enable(ENABLE_STREAMING);
        try {
            if (getReader() != null) {
                reader = factory.createJsonParser(getReader());
            } else if (getInputStream() != null) {
                reader = factory.createJsonParser(getInputStream());
            }
        } catch (IOException exception) {
            throw new FormatInputException(exception);
        }
        iterator = createInputIterator();
    }

    protected Iterator<ValueVariant[]> createInputIterator() {
        return new BsonInputIterator();
    }

    @Override
    protected void doReadBegin() {
        ValueModelList<ValueFormatModel> valueFormatModelList = createValueModelList();
        try {
            if (isNextToken(START_OBJECT) && isNextField(COLUMNS_FIELD) && isNextToken(START_OBJECT)) {
                while (true) {
                    ValueFormatModel valueFormatModel = new SimpleValueFormatModel();
                    if (isNextField(COLUMN_FIELD) && isNextToken(VALUE_STRING)) {
                        valueFormatModel.setName(reader.getText());
                    } else {
                        break;
                    }
                    if (isNextField(VARIANT_FIELD) && isNextToken(VALUE_STRING)) {
                        valueFormatModel.setValueVariantType(fromAlias(reader.getText()));
                    } else {
                        break;
                    }
                    valueFormatModelList.add(valueFormatModel);
                }
                reader.nextToken();
            }
            reader.nextToken();
            reader.nextToken();
        } catch (IOException exception) {
            throw new FormatInputException(exception);
        }
        setValueFormatModelList(valueFormatModelList);
    }

    protected boolean isCurrentToken(JsonToken... tokens) {
        return asList(tokens).contains(reader.getCurrentToken());
    }

    protected boolean isNextToken(JsonToken... tokens) throws IOException {
        return asList(tokens).contains(reader.nextToken());
    }

    protected boolean isNextField(String field) throws IOException {
        return isNextToken(JsonToken.FIELD_NAME) && field.equals(reader.getText());
    }

    @Override
    public boolean hasNextRow() {
        return iterator != null && iterator.hasNext();
    }

    protected ValueVariant[] readValues() {
        return iterator.next();
    }

    protected ValueVariant[] doReadValues() {
        ValueVariant[] values = null;
        try {
            if (isCurrentToken(START_ARRAY)) {
                ValueModelList<ValueFormatModel> valueFormatModelList = getValueFormatModelList();
                values = new ValueVariant[valueFormatModelList.size()];
                reader.nextToken();
                int index = 0;
                while (isCurrentToken(VALUE_NULL, VALUE_STRING, VALUE_EMBEDDED_OBJECT)) {
                    ValueVariantType valueVariantType = valueFormatModelList.get(index).getValueVariantType();
                    valueVariantType = valueVariantType != null ? valueVariantType : STRING;
                    switch (valueVariantType) {
                        case BINARY:
                            values[index] = binary((byte[]) reader.getEmbeddedObject());
                            break;
                        case STRING:
                            values[index] = string(valueOf(reader.getEmbeddedObject()));
                            break;
                    }
                    index++;
                    reader.nextToken();
                }
                reader.nextToken();
                fill(values, valueFormatModelList, index);
            }
        } catch (IOException exception) {
            throw new FormatInputException(exception);
        }
        return values;
    }

    @Override
    protected void doReadEnd() {
        try {
            reader.close();
        } catch (IOException exception) {
            throw new FormatInputException(exception);
        }
    }

    class BsonInputIterator implements Iterator<ValueVariant[]> {

        private ValueVariant[] current;

        @Override
        public boolean hasNext() {
            if (current == null) {
                current = doReadValues();
            }
            return current != null;
        }

        @Override
        public ValueVariant[] next() {
            ValueVariant[] next = current;
            current = null;
            if (next == null) {
                // hasNext() wasn't called before
                next = readValues();
                if (next == null) {
                    throw new FormatInputException("No more rows available");
                }
            }
            return next;
        }

        @Override
        public void remove() {
            throw new FormatInputException("Removal is unsupported operation");
        }
    }
}
