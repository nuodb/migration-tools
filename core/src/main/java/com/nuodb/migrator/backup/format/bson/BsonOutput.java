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

import com.fasterxml.jackson.core.JsonGenerator;
import com.nuodb.migrator.backup.Column;
import com.nuodb.migrator.backup.format.OutputBase;
import com.nuodb.migrator.backup.format.OutputException;
import com.nuodb.migrator.backup.format.value.Value;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.BitSet;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.backup.format.utils.BitSetUtils.toByteArray;
import static de.undercouch.bson4jackson.BsonGenerator.Feature.ENABLE_STREAMING;

/**
 * @author Sergey Bushik
 */
public class BsonOutput extends OutputBase implements BsonFormat {

    private JsonGenerator bsonWriter;

    public BsonOutput() {
        super(MAX_SIZE);
    }

    @Override
    public String getFormat() {
        return TYPE;
    }

    @Override
    protected void init(OutputStream output) {
        try {
            bsonWriter = createBsonFactory().createJsonGenerator(output);
        } catch (Exception exception) {
            throw new OutputException(exception);
        }
    }

    @Override
    protected void init(Writer writer) {
        try {
            bsonWriter = createBsonFactory().createJsonGenerator(writer);
        } catch (Exception exception) {
            throw new OutputException(exception);
        }
    }

    protected BsonFactory createBsonFactory() {
        BsonFactory factory = new BsonFactory();
        factory.enable(ENABLE_STREAMING);
        return factory;
    }

    @Override
    public void writeStart() {
        try {
            bsonWriter.writeStartObject();
            bsonWriter.writeArrayFieldStart(ROWS_FIELD);
        } catch (IOException exception) {
            throw new OutputException(exception);
        }
    }

    @Override
    public void writeValues(Value[] values) {
        try {
            bsonWriter.writeStartArray();
            BitSet nulls = new BitSet();
            for (int i = 0; i < values.length; i++) {
                nulls.set(i, values[i].isNull());
            }
            if (nulls.isEmpty()) {
                bsonWriter.writeNull();
            } else {
                bsonWriter.writeBinary(toByteArray(nulls));
            }
            Collection<Column> columns = getRowSet().getColumns();
            for (int i = 0; i < values.length; i++) {
                Value value = values[i];
                if (!value.isNull()) {
                    switch (get(columns, i).getValueType()) {
                    case BINARY:
                        bsonWriter.writeBinary(value.asBytes());
                        break;
                    case STRING:
                        bsonWriter.writeString(value.asString());
                        break;
                    }
                }
            }
            bsonWriter.writeEndArray();
        } catch (IOException exception) {
            throw new OutputException(exception);
        }
    }

    @Override
    public void writeEnd() {
        try {
            bsonWriter.writeEndArray();
            bsonWriter.writeEndObject();
            bsonWriter.flush();
        } catch (IOException exception) {
            throw new OutputException(exception);
        }
    }

    @Override
    public void close() {
        if (bsonWriter != null) {
            try {
                bsonWriter.close();
            } catch (IOException exception) {
                throw new OutputException(exception);
            }
            bsonWriter = null;
        }
    }
}
