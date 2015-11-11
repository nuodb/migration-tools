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

import com.nuodb.migrator.backup.format.Format;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

import java.util.Map;
import java.util.TreeMap;

import static java.lang.Boolean.parseBoolean;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.apache.commons.csv.CSVFormat.newFormat;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("ConstantConditions")
class CsvFormatBuilder implements CsvFormat {

    private static Map<String, Character> DELIMITERS;
    private static Map<String, String> LINE_SEPARATORS;

    static {
        DELIMITERS = new TreeMap<String, Character>(CASE_INSENSITIVE_ORDER);
        DELIMITERS.put(ATTRIBUTE_DELIMITER_TAB, DELIMITER_TAB);

        LINE_SEPARATORS = new TreeMap<String, String>(CASE_INSENSITIVE_ORDER);
        LINE_SEPARATORS.put(ATTRIBUTE_LINE_SEPARATOR_CRLF, LINE_SEPARATOR_CRLF);
        LINE_SEPARATORS.put(ATTRIBUTE_LINE_SEPARATOR_CR, LINE_SEPARATOR_CR);
        LINE_SEPARATORS.put(ATTRIBUTE_LINE_SEPARATOR_LF, LINE_SEPARATOR_LF);
    }

    private Format format;

    private Character delimiter;
    private Character quote;
    private Character escape;
    private Character commentMarker;
    private String lineSeparator;
    private boolean quoting;

    public CsvFormatBuilder(Format format) {
        this.format = format;
    }

    public CSVFormat build() {
        CSVFormat format = newFormat(delimiter = initDelimiter());
        format = format.withEscape(escape = initEscape());
        format = format.withCommentMarker(commentMarker = initCommentMarker());
        format = format.withRecordSeparator(lineSeparator = initLineSeparator());
        quote = initQuote();
        quoting = initQuoting();
        if (quoting) {
            format = format.withQuoteMode(QuoteMode.MINIMAL);
            format = format.withQuote(quote);
        }
        return format;
    }

    protected Character initEscape() {
        String escapeValue = (String) format.getAttribute(ATTRIBUTE_ESCAPE);
        if (isEmpty(escapeValue)) {
            escape = ESCAPE;
        } else {
            escape = escapeValue.charAt(0);
        }
        return escape;
    }

    protected Character initQuote() {
        Character quote;
        String quoteValue = (String) format.getAttribute(ATTRIBUTE_QUOTE);
        if (isEmpty(quoteValue)) {
            quote = QUOTE;
        } else {
            quote = quoteValue.charAt(0);
        }
        return quote;
    }

    protected boolean initQuoting() {
        boolean quoting;
        String quotingValue = (String) format.getAttribute(ATTRIBUTE_QUOTING);
        if (isEmpty(quotingValue)) {
            quoting = QUOTING;
        } else {
            quoting = parseBoolean(quotingValue);
        }
        return quoting;
    }

    protected Character initDelimiter() {
        Character delimiter = null;
        String delimiterValue = (String) format.getAttribute(ATTRIBUTE_DELIMITER);
        if (delimiterValue != null) {
            delimiter = DELIMITERS.get(delimiterValue);
        }
        if (delimiter == null) {
            if (isEmpty(delimiterValue)) {
                delimiter = DELIMITER;
            } else {
                delimiter = delimiterValue.charAt(0);
            }
        }
        return delimiter;
    }

    protected String initLineSeparator() {
        String lineSeparator = null;
        String lineSeparatorValue = (String) format.getAttribute(ATTRIBUTE_LINE_SEPARATOR);
        if (lineSeparatorValue != null) {
            lineSeparator = LINE_SEPARATORS.get(lineSeparatorValue);
        }
        if (lineSeparator == null) {
            if (isEmpty(lineSeparatorValue)) {
                lineSeparator = LINE_SEPARATOR;
            } else {
                lineSeparator = lineSeparatorValue;
            }
        }
        return lineSeparator;
    }

    protected Character initCommentMarker() {
        return COMMENT_MARKER;
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

    public Character getCommentMarker() {
        return commentMarker;
    }
}
