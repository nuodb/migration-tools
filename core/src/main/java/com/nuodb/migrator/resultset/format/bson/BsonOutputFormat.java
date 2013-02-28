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

import com.fasterxml.jackson.core.JsonGenerator;
import com.nuodb.migrator.resultset.format.FormatOutputBase;
import com.nuodb.migrator.resultset.format.FormatOutputException;
import com.nuodb.migrator.resultset.format.value.ValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueVariant;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.IOException;

import static com.nuodb.migrator.resultset.format.value.ValueVariantType.toAlias;
import static de.undercouch.bson4jackson.BsonGenerator.Feature.ENABLE_STREAMING;

/**
 * @author Sergey Bushik
 */
public class BsonOutputFormat extends FormatOutputBase implements BsonAttributes {

    private JsonGenerator writer;

    @Override
    public String getType() {
        return FORMAT_TYPE;
    }

    @Override
    public void initOutput() {
        BsonFactory factory = new BsonFactory();
        factory.enable(ENABLE_STREAMING);
        try {
            if (getWriter() != null) {
                writer = factory.createJsonGenerator(getWriter());
            } else if (getOutputStream() != null) {
                writer = factory.createJsonGenerator(getOutputStream());
            }
        } catch (IOException exception) {
            throw new FormatOutputException(exception);
        }

    }

    @Override
    protected void doWriteBegin() {
        try {
            writer.writeStartObject();
            writer.writeFieldName(COLUMNS_FIELD);
            writer.writeStartObject();
            for (ValueFormatModel valueFormatModel : getValueFormatModelList()) {
                writer.writeStringField(COLUMN_FIELD, valueFormatModel.getName());
                writer.writeStringField(VARIANT_FIELD, toAlias(valueFormatModel.getValueVariantType()));
            }
            writer.writeEndObject();
            writer.writeFieldName(ROWS_FIELD);
            writer.writeStartArray();
        } catch (IOException exception) {
            throw new FormatOutputException(exception);
        }
    }

    @Override
    protected void writeValues(ValueVariant[] variants) {
        try {
            writer.writeStartArray();
            int i = 0;
            for (ValueVariant variant : variants) {
                if (variant.isNull()) {
                    writer.writeNull();
                } else {
                    switch (getValueFormatModelList().get(i).getValueVariantType()) {
                        case BINARY:
                            writer.writeBinary(variant.asBytes());
                            break;
                        case STRING:
                            writer.writeString(variant.asString());
                            break;
                    }
                }
                i++;
            }
            writer.writeEndArray();
        } catch (IOException exception) {
            throw new FormatOutputException(exception);
        }
    }

    @Override
    protected void doWriteEnd() {
        try {
            writer.writeEndArray();
            writer.writeEndObject();
            writer.flush();
            writer.close();
        } catch (IOException exception) {
            throw new FormatOutputException(exception);
        }
    }
}
