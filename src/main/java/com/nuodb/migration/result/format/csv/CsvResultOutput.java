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
package com.nuodb.migration.result.format.csv;

import com.nuodb.migration.result.format.ResultInputException;
import com.nuodb.migration.result.format.ResultOutputBase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.PrintWriter;

import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class CsvResultOutput extends ResultOutputBase implements CsvAttributes {

    private CSVPrinter printer;
    private String doubleQuote;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected void doInitOutput() {
        CsvFormatBuilder builder = new CsvFormatBuilder(this);
        CSVFormat format = builder.build();
        Character quote = builder.getQuote();
        doubleQuote = valueOf(quote) + valueOf(quote);

        if (getWriter() != null) {
            printer = new CSVPrinter(getWriter(), format);
        } else if (getOutputStream() != null) {
            printer = new CSVPrinter(new PrintWriter(getOutputStream()), format);
        }
    }

    @Override
    protected void doWriteBegin() {
        try {
            printer.printRecord(getValueSetModel().getNames());
        } catch (IOException e) {
            throw new ResultInputException(e);
        }
    }

    @Override
    protected void doWriteRow(String[] columnValues) {
        try {
            for (int i = 0; i < columnValues.length; i++) {
                String column = columnValues[i];
                if (column != null && column.length() == 0) {
                    columnValues[i] = doubleQuote;
                }
            }
            printer.printRecord(columnValues);
        } catch (IOException e) {
            throw new ResultInputException(e);
        }
    }

    @Override
    protected void doWriteEnd() {
        try {
            printer.flush();
        } catch (IOException e) {
            throw new ResultInputException(e);
        }
    }
}
