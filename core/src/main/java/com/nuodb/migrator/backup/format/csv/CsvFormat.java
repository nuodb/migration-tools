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

import static java.lang.System.getProperty;

/**
 * @author Sergey Bushik
 */
public interface CsvFormat {
    /**
     * Input & output stream encoding
     */
    final String ATTRIBUTE_ENCODING = "csv.encoding";
    /**
     * The symbol used for value separation, must not be a line break character.
     */
    final String ATTRIBUTE_DELIMITER = "csv.delimiter";
    /**
     * Indicates whether quotation should be used.
     */
    final String ATTRIBUTE_QUOTING = "csv.quoting";
    /**
     * The symbol used as value encapsulation marker.
     */
    final String ATTRIBUTE_QUOTE = "csv.quote";
    /**
     * The symbol used to escape special characters in values.
     */
    final String ATTRIBUTE_ESCAPE = "csv.escape";
    /**
     * The record separator to use.
     */
    final String ATTRIBUTE_LINE_SEPARATOR = "csv.line.separator";

    final String TYPE = "csv";
    final String ATTRIBUTE_LINE_SEPARATOR_CRLF = "CRLF";
    final String ATTRIBUTE_LINE_SEPARATOR_CR = "CR";
    final String ATTRIBUTE_LINE_SEPARATOR_LF = "LF";

    final String LINE_SEPARATOR_CRLF = "\r\n";
    final String LINE_SEPARATOR_CR = "\r";
    final String LINE_SEPARATOR_LF = "\n";
    final String LINE_SEPARATOR = getProperty("line.separator");

    final String ATTRIBUTE_DELIMITER_TAB = "tab";
    final Character DELIMITER_TAB = '\t';
    final Character DELIMITER_COMMA = ',';
    final Character DELIMITER = DELIMITER_COMMA;

    final char COMMENT_MARKER = '#';
    final String ENCODING = getProperty("file.encoding");
    final boolean QUOTING = false;
    final Character QUOTE = '"';
    final Character ESCAPE = '|';
}
