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
package com.nuodb.migrator.jdbc.metadata.generator;

import java.io.BufferedWriter;
import java.io.Writer;

/**
 * @author Sergey Bushik
 */
public abstract class StreamScriptExporterBase extends ScriptExporterBase implements StreamScriptProcessor {

    private String encoding = ENCODING;
    private String delimiter = DELIMITER;
    private String commentStart = COMMENT_START;

    private transient BufferedWriter writer;

    @Override
    protected void doOpen() throws Exception {
        writer = new BufferedWriter(openWriter());
    }

    protected abstract Writer openWriter() throws Exception;

    @Override
    protected void doExportScript(Script script) throws Exception {
        writer.write(script.getSQL());
        String delimiter = getDelimiter();
        if (!script.getSQL().endsWith(delimiter)) {
            writer.write(delimiter);
        }
        writer.newLine();
        writer.flush();
    }

    @Override
    protected void doClose() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public String getEncoding() {
        return encoding;
    }

    @Override
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Override
    public String getDelimiter() {
        return delimiter;
    }

    @Override
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String getCommentStart() {
        return commentStart;
    }

    @Override
    public void setCommentStart(String commentStart) {
        this.commentStart = commentStart;
    }
}
