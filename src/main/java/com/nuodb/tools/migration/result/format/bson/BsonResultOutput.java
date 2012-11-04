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
package com.nuodb.tools.migration.result.format.bson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.nuodb.tools.migration.jdbc.metamodel.ColumnSetModel;
import com.nuodb.tools.migration.result.format.ResultInputException;
import com.nuodb.tools.migration.result.format.ResultOutputBase;
import de.undercouch.bson4jackson.BsonFactory;

import java.io.IOException;

import static de.undercouch.bson4jackson.BsonGenerator.Feature.ENABLE_STREAMING;

/**
 * @author Sergey Bushik
 */
public class BsonResultOutput extends ResultOutputBase implements BsonAttributes {

    private JsonGenerator generator;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected void doInitOutput() {
        BsonFactory generatorFactory = new BsonFactory();
        generatorFactory.enable(ENABLE_STREAMING);
        try {
            if (getWriter() != null) {
                generator = generatorFactory.createJsonGenerator(getWriter());
            } else if (getOutputStream() != null) {
                generator = generatorFactory.createJsonGenerator(getOutputStream());
            }
        } catch (IOException exception) {
            throw new ResultInputException(exception);
        }

    }

    @Override
    protected void doWriteBegin() {
        try {
            generator.writeStartObject();
            generator.writeFieldName(COLUMNS_FIELD);
            generator.writeStartObject();
            ColumnSetModel columnSetModel = getColumnSetModel();
            for (int index = 0; index < columnSetModel.getColumnCount(); index++) {
                generator.writeStringField(COLUMN_FIELD, columnSetModel.getColumn(index));
            }
            generator.writeEndObject();
            generator.writeFieldName(ROWS_FIELD);
            generator.writeStartArray();
        } catch (IOException exception) {
            throw new ResultInputException(exception);
        }
    }

    @Override
    protected void doWriteRow(String[] values) {
        try {
            generator.writeStartArray();
            for (String value : values) {
                if (value == null) {
                    generator.writeNull();
                } else {
                    generator.writeString(value);
                }
            }
            generator.writeEndArray();
        } catch (IOException exception) {
            throw new ResultInputException(exception);
        }
    }

    @Override
    protected void doWriteEnd() {
        try {
            generator.writeEndArray();
            generator.writeEndObject();
            generator.flush();
            generator.close();
        } catch (IOException exception) {
            throw new ResultInputException(exception);
        }
    }
}
