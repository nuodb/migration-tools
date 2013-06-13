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
package com.nuodb.migrator.jdbc.metadata.generator;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import static java.lang.System.getProperty;

/**
 * @author Sergey Bushik
 */
public class WriterScriptExporter extends ScriptExporterBase {

    public static final ScriptExporter SYSTEM_OUT = new WriterScriptExporter(System.out, false);

    private static final String SEMICOLON = ";";

    private transient final Writer writer;
    private final boolean close;
    private String endOfLine = SEMICOLON;
    private String lineSeparator = getProperty("line.separator");

    public WriterScriptExporter(OutputStream outputStream) {
        this(outputStream, true);
    }

    public WriterScriptExporter(Writer writer) {
        this(writer, true);
    }

    public WriterScriptExporter(OutputStream outputStream, boolean close) {
        this(new OutputStreamWriter(outputStream), close);
    }

    public WriterScriptExporter(Writer writer, boolean close) {
        this.writer = writer;
        this.close = close;
    }

    @Override
    protected void doOpen() throws Exception {
    }

    @Override
    protected void doExportScript(String script) throws Exception {
        writer.write(script);
        if (!script.endsWith(endOfLine)) {
            writer.write(endOfLine);
        }
        writer.write(lineSeparator);
    }

    @Override
    protected void doClose() throws Exception {
        writer.flush();
        if (close) {
            writer.close();
        }
    }

    public String getEndOfLine() {
        return endOfLine;
    }

    public void setEndOfLine(String endOfLine) {
        this.endOfLine = endOfLine;
    }

    public String getLineSeparator() {
        return lineSeparator;
    }

    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }
}
