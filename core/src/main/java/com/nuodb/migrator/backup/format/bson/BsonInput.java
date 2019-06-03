/**
 * Copyright (c) 2015, NuoDB, Inc.
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
package com.nuodb.migrator.backup.format.bson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.nuodb.migrator.backup.format.InputBase;
import com.nuodb.migrator.backup.format.InputException;
import com.nuodb.migrator.backup.format.value.Value;
import com.nuodb.migrator.backup.format.value.ValueType;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.BitSet;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.nuodb.migrator.backup.format.utils.BitSetUtils.EMPTY;
import static com.nuodb.migrator.backup.format.utils.BitSetUtils.fromByteArray;
import static com.nuodb.migrator.backup.format.value.ValueType.STRING;
import static com.nuodb.migrator.backup.format.value.ValueUtils.binary;
import static com.nuodb.migrator.backup.format.value.ValueUtils.string;
import static de.undercouch.bson4jackson.BsonGenerator.Feature.ENABLE_STREAMING;

/**
 * @author Sergey Bushik
 */
public class BsonInput extends InputBase implements BsonFormat {

    private JsonParser bsonReader;

    @Override
    public String getFormat() {
        return TYPE;
    }

    @Override
    protected void init(Reader reader) {
        try {
            bsonReader = createBsonFactory().createJsonParser(reader);
        } catch (Exception exception) {
            throw new InputException(exception);
        }
    }

    @Override
    protected void init(InputStream input) {
        try {
            bsonReader = createBsonFactory().createJsonParser(input);
        } catch (Exception exception) {
            throw new InputException(exception);
        }
    }

    protected BsonFactory createBsonFactory() {
        BsonFactory factory = new BsonFactory();
        factory.enable(ENABLE_STREAMING);
        return factory;
    }

    @Override
    public void readStart() {
        try {
            bsonReader.nextToken();
            bsonReader.nextToken();
            bsonReader.nextToken();
        } catch (IOException exception) {
            throw new InputException(exception);
        }
    }

    @Override
    public Value[] readValues() {
        Value[] values = null;
        try {
            if (isNextToken(START_ARRAY)) {
                List<ValueType> valueTypes = getValueTypes();
                int length = valueTypes.size();
                values = new Value[length];
                int index = 0;
                BitSet nulls = isNextToken(VALUE_NULL) ? EMPTY : fromByteArray((byte[]) bsonReader.getEmbeddedObject());
                while (index < length) {
                    Object value;
                    if (nulls.get(index)) {
                        value = null;
                    } else {
                        bsonReader.nextToken();
                        value = bsonReader.getEmbeddedObject();
                    }
                    ValueType valueType = valueTypes.get(index);
                    valueType = valueType != null ? valueType : STRING;
                    switch (valueType) {
                    case BINARY:
                        values[index] = binary((byte[]) value);
                        break;
                    case STRING:
                        values[index] = string((String) value);
                        break;
                    }
                    index++;
                }
                bsonReader.nextToken();
            }
        } catch (IOException exception) {
            throw new InputException(exception);
        }
        return values;
    }

    protected boolean isNextToken(JsonToken... tokens) throws IOException {
        return isToken(bsonReader.nextToken(), tokens);
    }

    protected boolean isToken(JsonToken target, JsonToken... tokens) {
        for (JsonToken token : tokens) {
            if (token == target) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void readEnd() {
    }

    @Override
    public void close() {
        if (bsonReader != null) {
            try {
                bsonReader.close();
            } catch (IOException exception) {
                throw new InputException(exception);
            }
            bsonReader = null;
        }
    }
}
