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


import com.google.common.io.Files;

import java.io.BufferedWriter;
import java.io.File;

import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.nio.charset.Charset.forName;

/**
 * @author Sergey Bushik
 */
public class FileScriptExporter extends CountingScriptExporter {

    private static final String SEMICOLON = ";";
    private File file;
    private String encoding;
    private BufferedWriter writer;

    public FileScriptExporter(String file) {
        this(file, null);
    }

    public FileScriptExporter(String file, String encoding) {
        this(new File(file), encoding);
    }

    public FileScriptExporter(File file) {
        this(file, null);
    }

    public FileScriptExporter(File file, String encoding) {
        this.file = file;
        this.encoding = encoding != null ? encoding : getProperty("file.encoding");
    }

    @Override
    protected void doOpen() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug(format("Opening file to export scripts to %s", file));
        }
        Files.createParentDirs(file);
        writer = Files.newWriter(file, forName(encoding));
    }

    @Override
    public void exportScript(String script) throws Exception {
        if (writer == null) {
            throw new GeneratorException("File is not opened");
        }
        if (getCount() > 0) {
            writer.newLine();
        }
        writer.write(script);
        if (!script.endsWith(SEMICOLON)) {
            writer.write(SEMICOLON);
        }
    }

    @Override
    protected void doClose() throws Exception {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }
}