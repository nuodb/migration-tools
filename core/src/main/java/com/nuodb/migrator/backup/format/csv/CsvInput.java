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
package com.nuodb.migrator.backup.format.csv;

import com.nuodb.migrator.backup.format.InputBase;
import com.nuodb.migrator.backup.format.InputException;
import com.nuodb.migrator.backup.format.value.Value;
import com.nuodb.migrator.backup.format.value.ValueType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.backup.format.utils.BinaryEncoder.BASE64;
import static com.nuodb.migrator.backup.format.value.ValueType.STRING;
import static com.nuodb.migrator.backup.format.value.ValueUtils.*;
import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
public class CsvInput extends InputBase implements CsvFormat {

    private String doubleQuote;
    private Iterator<CSVRecord> iterator;
    private CSVParser parser;

    @Override
    public String getFormat() {
        return TYPE;
    }

    @Override
    protected void init(InputStream inputStream) {
        try {
            init(new InputStreamReader(getInputStream(), (String) getAttribute(ATTRIBUTE_ENCODING, ENCODING)));
        } catch (UnsupportedEncodingException exception) {
            throw new InputException(exception);
        }
    }

    @Override
    protected void init(Reader reader) {
        CsvFormatBuilder builder = new CsvFormatBuilder(this);
        CSVFormat format = builder.build();
        Character quote = builder.getQuote();

        doubleQuote = valueOf(quote) + valueOf(quote);
        try {
            parser = new CSVParser(reader, format);
            iterator = parser.iterator();
        } catch (IOException exception) {
            throw new InputException(exception);
        }
    }

    @Override
    public void readStart() {
        if (iterator.hasNext()) {
            iterator.next();
        }
    }

    @Override
    public Value[] readValues() {
        return iterator.hasNext() ? readRow() : null;
    }

    protected Value[] readRow() {
        CSVRecord record = iterator.next();
        List<ValueType> valueTypes = getValueTypes();
        Value[] values = new Value[valueTypes.size()];
        int index = 0;
        for (String value : newArrayList(record.iterator())) {
            if (doubleQuote.equals(value)) {
                value = StringUtils.EMPTY;
            } else if (value != null && value.length() == 0) {
                value = null;
            }
            ValueType type = valueTypes.get(index);
            type = type != null ? type : STRING;
            switch (type) {
            case BINARY:
                values[index] = binary(BASE64.decode(value));
                break;
            case STRING:
                values[index] = string(value);
                break;
            }
            index++;
        }
        fill(values, valueTypes, index);
        return values;
    }

    @Override
    public void readEnd() {
    }

    @Override
    public void close() {
        if (parser != null) {
            try {
                parser.close();
            } catch (IOException exception) {
                throw new InputException(exception);
            }
            parser = null;
        }
    }
}
