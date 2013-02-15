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

import com.nuodb.migration.resultset.format.FormatOutputBase;
import com.nuodb.migration.resultset.format.FormatOutputException;
import com.nuodb.migration.resultset.format.utils.BinaryEncoder;
import com.nuodb.migration.resultset.format.value.ValueFormatModel;
import com.nuodb.migration.resultset.format.value.ValueVariant;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import static com.nuodb.migration.resultset.format.value.ValueVariantType.toAlias;
import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class CsvOutputFormat extends FormatOutputBase implements CsvAttributes {

    private CSVPrinter printer;
    private String doubleQuote;

    @Override
    public String getType() {
        return FORMAT_TYPE;
    }

    @Override
    public void initOutput() {
        CsvFormatBuilder builder = new CsvFormatBuilder(this);
        CSVFormat format = builder.build();
        Character quote = builder.getQuote();
        doubleQuote = valueOf(quote) + valueOf(quote);

        if (getWriter() != null) {
            printer = new CSVPrinter(getWriter(), format);
        } else if (getOutputStream() != null) {
            String encoding = (String) getAttribute(ATTRIBUTE_ENCODING, ENCODING);
            printer = new CSVPrinter(new OutputStreamWriter(getOutputStream(), Charset.forName(encoding)), format);
        }
    }

    @Override
    protected void doWriteBegin() {
        try {
            final StringBuilder variant = new StringBuilder();
            int i = 0;
            for (ValueFormatModel valueFormatModel : getValueFormatModelList()) {
                if (i++ > 0) {
                    variant.append(",");
                }
                variant.append(toAlias(valueFormatModel.getValueVariantType()));
            }
            printer.printComment(variant.toString());
            for (ValueFormatModel valueFormatModel : getValueFormatModelList()) {
                printer.print(valueFormatModel.getName());

            }
            printer.println();
        } catch (IOException e) {
            throw new FormatOutputException(e);
        }
    }

    @Override
    protected void writeValues(ValueVariant[] variants) {
        try {
            String[] values = new String[variants.length];
            for (int i = 0; i < variants.length; i++) {
                ValueVariant variant = variants[i];
                String value = null;
                switch (getValueFormatModelList().get(i).getValueVariantType()) {
                    case BINARY:
                        value = BinaryEncoder.HEX.encode(variant.asBytes());
                        break;
                    case STRING:
                        value = variant.asString();
                        break;
                }
                if (value != null && value.length() == 0) {
                    value = doubleQuote;
                }
                values[i] = value;
            }
            printer.printRecord(values);
        } catch (IOException e) {
            throw new FormatOutputException(e);
        }
    }

    @Override
    protected void doWriteEnd() {
        try {
            printer.close();
        } catch (IOException e) {
            throw new FormatOutputException(e);
        }
    }
}
