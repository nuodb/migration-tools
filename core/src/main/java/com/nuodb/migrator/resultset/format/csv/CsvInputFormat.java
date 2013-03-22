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
package com.nuodb.migrator.resultset.format.csv;

import com.nuodb.migrator.jdbc.model.ValueModelList;
import com.nuodb.migrator.resultset.format.FormatInputBase;
import com.nuodb.migrator.resultset.format.FormatInputException;
import com.nuodb.migrator.resultset.format.value.SimpleValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueVariant;
import com.nuodb.migrator.resultset.format.value.ValueVariantType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.model.ValueModelFactory.createValueModelList;
import static com.nuodb.migrator.resultset.format.utils.BinaryEncoder.BASE64;
import static com.nuodb.migrator.resultset.format.value.ValueVariantType.STRING;
import static com.nuodb.migrator.resultset.format.value.ValueVariantType.fromAlias;
import static com.nuodb.migrator.resultset.format.value.ValueVariantUtils.fill;
import static com.nuodb.migrator.resultset.format.value.ValueVariants.binary;
import static com.nuodb.migrator.resultset.format.value.ValueVariants.string;
import static java.lang.String.valueOf;
import static java.nio.charset.Charset.forName;
import static org.apache.commons.lang3.StringUtils.split;
import static org.apache.commons.lang3.StringUtils.trim;

/**
 * @author Sergey Bushik
 */
public class CsvInputFormat extends FormatInputBase implements CsvAttributes {

    private CSVParser parser;
    private String doubleQuote;
    private Iterator<CSVRecord> iterator;

    @Override
    public String getType() {
        return FORMAT_TYPE;
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
                String encoding = (String) getAttribute(ATTRIBUTE_ENCODING, ENCODING);
                parser = new CSVParser(new InputStreamReader(getInputStream(), forName(encoding)), format);
            }
        } catch (IOException exception) {
            throw new FormatInputException(exception);
        }
        iterator = parser.iterator();
    }

    @Override
    protected void doReadBegin() {
        ValueModelList<ValueFormatModel> list = createValueModelList();
        if (iterator.hasNext()) {
            final CSVRecord record = iterator.next();
            String[] aliases = split(record.getComment(), ",");
            int i = 0;
            for (String column : record) {
                ValueFormatModel model = new SimpleValueFormatModel();
                model.setName(column);
                if (aliases != null && i < aliases.length) {
                    model.setValueVariantType(fromAlias(trim(aliases[i])));
                }
                list.add(model);
                i++;
            }
        }
        setValueFormatModelList(list);
    }

    @Override
    public boolean hasNextRow() {
        return iterator.hasNext();
    }

    @Override
    protected ValueVariant[] readValues() {
        CSVRecord record = iterator.next();
        ValueModelList<ValueFormatModel> list = getValueFormatModelList();
        ValueVariant[] values = new ValueVariant[list.size()];
        int index = 0;
        for (String value : newArrayList(record.iterator())) {
            if (doubleQuote.equals(value)) {
                value = StringUtils.EMPTY;
            } else if (value != null && value.length() == 0) {
                value = null;
            }
            ValueVariantType type = list.get(index).getValueVariantType();
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
        fill(values, list, index);
        return values;
    }

    @Override
    protected void doReadEnd() {
    }
}
