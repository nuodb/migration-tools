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
package com.nuodb.tools.migration.result.format.csv;

import com.nuodb.tools.migration.result.format.ResultFormatException;
import com.nuodb.tools.migration.result.format.ResultOutputBase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.Quote;

import java.io.IOException;
import java.io.PrintWriter;

import static java.lang.Boolean.parseBoolean;
import static java.lang.String.valueOf;
import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class CsvResultOutput extends ResultOutputBase implements CsvResultFormat {

    private CSVPrinter printer;
    /**
     *  The symbol used for value separation, must not be a line break character.
     */
    private Character delimiter;
    /**
     * Indicates whether quotation should be used.
     */
    private boolean quoting;
    /**
     * The symbol used as value encapsulation marker.
     */
    private Character quote;
    /**
     * The symbol used to escape special characters in values.
     */
    private Character escape;
    /**
     * The record separator to use for withConnection.
     */
    private String lineSeparator;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected void doSetAttributes() {
        String delimiterValue = getAttribute(ATTRIBUTE_DELIMITER);
        if (isEmpty(delimiterValue)) {
            delimiter = DELIMITER;
        } else {
            delimiter = delimiterValue.charAt(0);
        }

        String quotingValue = getAttribute(ATTRIBUTE_QUOTING);
        if (isEmpty(quotingValue)){
            quoting = QUOTING;
        } else {
            quoting = parseBoolean(quotingValue);
        }

        String quoteValue = getAttribute(ATTRIBUTE_QUOTE);
        if (isEmpty(quoteValue)) {
            quote = QUOTE;
        } else {
            quote = quoteValue.charAt(0);
        }

        String escapeValue = getAttribute(ATTRIBUTE_ESCAPE);
        if (isEmpty(escapeValue)) {
            escape = ESCAPE;
        } else {
            escape = escapeValue.charAt(0);
        }
        lineSeparator = getAttribute(ATTRIBUTE_LINE_SEPARATOR, LINE_SEPARATOR);
    }

    @Override
    protected void doOutputStart() {
        CSVFormat format = new CSVFormat(delimiter);
        if (quoting && quote != null) {
            format = format.withQuotePolicy(Quote.MINIMAL);
            format = format.withQuoteChar(quote);
        }
        format = format.withRecordSeparator(lineSeparator);
        format = format.withEscape(escape);

        if (getWriter() != null) {
            printer = new CSVPrinter(getWriter(), format);
        } else if (getOutputStream() != null) {
            printer = new CSVPrinter(new PrintWriter(getOutputStream()), format);
        }
        try {
            printer.printRecord(getColumnSetModel().getColumns());
        } catch (IOException e) {
            throw new ResultFormatException(e);
        }
    }

    @Override
    protected void doOutputRow() {
        try {
            String[] columns = getRowValues();
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (column != null && column.length() == 0) {
                    columns[i] = valueOf(quote) + valueOf(quote);
                }
            }
            printer.printRecord(columns);
        } catch (IOException e) {
            throw new ResultFormatException(e);
        }
    }

    @Override
    protected void doOutputEnd() {
        try {
            printer.flush();
        } catch (IOException e) {
            throw new ResultFormatException(e);
        }
    }
}
