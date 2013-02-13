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

import com.nuodb.migration.jdbc.model.ValueModelList;
import com.nuodb.migration.resultset.format.ResultSetInputBase;
import com.nuodb.migration.resultset.format.ResultSetInputException;
import com.nuodb.migration.resultset.format.utils.BinaryEncoder;
import com.nuodb.migration.resultset.format.value.SimpleValueFormatModel;
import com.nuodb.migration.resultset.format.value.ValueFormatModel;
import com.nuodb.migration.resultset.format.value.ValueVariant;
import com.nuodb.migration.resultset.format.value.ValueVariantType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migration.jdbc.model.ValueModelFactory.createValueModelList;
import static com.nuodb.migration.resultset.format.value.ValueVariantType.STRING;
import static com.nuodb.migration.resultset.format.value.ValueVariantType.fromAlias;
import static com.nuodb.migration.resultset.format.value.ValueVariants.binary;
import static com.nuodb.migration.resultset.format.value.ValueVariants.string;
import static java.lang.String.valueOf;
import static org.apache.commons.lang3.StringUtils.split;
import static org.apache.commons.lang3.StringUtils.trim;

/**
 * @author Sergey Bushik
 */
public class CsvResultSetInput extends ResultSetInputBase implements CsvAttributes {

    private CSVParser parser;
    private String doubleQuote;
    private Iterator<CSVRecord> iterator;

    @Override
    public String getFormat() {
        return FORMAT;
    }

    @Override
    public void initInput() {
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
        ValueModelList<ValueFormatModel> valueFormatModelList = createValueModelList();
        if (iterator.hasNext()) {
            final CSVRecord record = iterator.next();
            String[] aliases = split(record.getComment(), ",");
            int i = 0;
            for (String column : record) {
                ValueFormatModel valueFormatModel = new SimpleValueFormatModel();
                valueFormatModel.setName(column);
                if (aliases != null && i < aliases.length) {
                    valueFormatModel.setValueVariantType(fromAlias(trim(aliases[i])));
                }
                valueFormatModelList.add(valueFormatModel);
                i++;
            }
        }
        setValueFormatModelList(valueFormatModelList);
    }

    @Override
    public boolean hasNextRow() {
        return iterator.hasNext();
    }

    @Override
    public void readRow() {
        final CSVRecord record = iterator.next();
        ValueModelList<ValueFormatModel> valueFormatModelList = getValueFormatModelList();
        ValueVariant[] values = new ValueVariant[valueFormatModelList.size()];
        int i = 0;
        for (String value : newArrayList(record.iterator())) {
            if (doubleQuote.equals(value)) {
                value = StringUtils.EMPTY;
            } else if (value != null && value.length() == 0) {
                value = null;
            }
            ValueVariantType valueVariantType = valueFormatModelList.get(i).getValueVariantType();
            valueVariantType = valueVariantType != null ? valueVariantType : STRING;
            switch (valueVariantType) {
                case BINARY:
                    values[i] = binary(BinaryEncoder.HEX.decode(value));
                    break;
                case STRING:
                    values[i] = string(value);
                    break;
            }
            i++;
        }
        setValues(values);
    }

    @Override
    protected void doReadEnd() {
    }
}
