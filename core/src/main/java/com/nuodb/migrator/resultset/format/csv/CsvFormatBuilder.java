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

import com.nuodb.migrator.resultset.format.Format;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.Quote;

import static java.lang.Boolean.parseBoolean;
import static org.apache.commons.csv.CSVFormat.CSVFormatBuilder;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
class CsvFormatBuilder implements CsvAttributes {

    private Format format;

    private Character delimiter;
    private boolean quoting;
    private Character quote;
    private Character escape;
    private String lineSeparator;

    public CsvFormatBuilder(Format format) {
        this.format = format;
    }

    public CSVFormat build() {
        String delimiterValue = (String) format.getAttribute(ATTRIBUTE_DELIMITER);
        if (isEmpty(delimiterValue)) {
            delimiter = DELIMITER;
        } else {
            if (delimiterValue.equalsIgnoreCase(ATTRIBUTE_DELIMITER_TAB)) {
                delimiter = TAB;
            } else {
                delimiter = delimiterValue.charAt(0);
            }
        }
        String quotingValue = (String) format.getAttribute(ATTRIBUTE_QUOTING);
        if (isEmpty(quotingValue)) {
            quoting = QUOTING;
        } else {
            quoting = parseBoolean(quotingValue);
        }

        String quoteValue = (String) format.getAttribute(ATTRIBUTE_QUOTE);
        if (isEmpty(quoteValue)) {
            quote = QUOTE;
        } else {
            quote = quoteValue.charAt(0);
        }

        String escapeValue = (String) format.getAttribute(ATTRIBUTE_ESCAPE);
        if (isEmpty(escapeValue)) {
            escape = ESCAPE;
        } else {
            escape = escapeValue.charAt(0);
        }
        lineSeparator = (String) format.getAttribute(ATTRIBUTE_LINE_SEPARATOR, LINE_SEPARATOR);

        CSVFormatBuilder builder = CSVFormat.newBuilder(delimiter);
        if (quoting && quote != null) {
            builder.withQuotePolicy(Quote.MINIMAL);
            builder.withQuoteChar(quote);
        }
        builder.withCommentStart(COMMENT_START);
        builder.withRecordSeparator(lineSeparator);
        builder.withEscape(escape);
        return builder.build();
    }

    public Character getDelimiter() {
        return delimiter;
    }

    public boolean isQuoting() {
        return quoting;
    }

    public Character getQuote() {
        return quote;
    }

    public Character getEscape() {
        return escape;
    }

    public String getLineSeparator() {
        return lineSeparator;
    }
}
