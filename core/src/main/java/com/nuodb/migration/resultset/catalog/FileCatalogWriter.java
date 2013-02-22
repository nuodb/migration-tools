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
package com.nuodb.migration.resultset.catalog;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.getProperty;
import static org.apache.commons.io.FileUtils.*;
import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * @author Sergey Bushik
 */
public class FileCatalogWriter implements CatalogWriter {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private OutputStream output;
    private List<CatalogEntry> entries = Lists.newArrayList();

    private File catalogDir;
    private File catalogFile;

    public FileCatalogWriter(File catalogDir, File catalogFile) {
        this.catalogDir = catalogDir;
        this.catalogFile = catalogFile;
    }

    @Override
    public void write(CatalogEntry catalogEntry) {
        try {
            initOutput();
            String entryName = valueOf(catalogEntry);
            if (logger.isTraceEnabled()) {
                logger.trace(format("Adding entry %1$s", entryName));
            }
            if (!entries.isEmpty()) {
                IOUtils.write(getProperty("line.separator"), output);
            }
            entries.add(catalogEntry);
            IOUtils.write(entryName, output);
        } catch (IOException e) {
            throw new CatalogException("Failed writing entry to catalog", e);
        }
    }

    protected void initOutput() {
        if (output == null) {
            output = createOutput();
        }
    }

    protected OutputStream createOutput() {
        if (logger.isDebugEnabled()) {
            logger.debug(format("Opening entry catalog writer %s", catalogFile));
        }
        try {
            forceMkdir(catalogDir);
        } catch (IOException e) {
            throw new CatalogException("Can't open dump catalog dir", e);
        }
        try {
            touch(catalogFile);
        } catch (IOException e) {
            throw new CatalogException("Can't open dump catalog file", e);
        }
        try {
            return openOutputStream(catalogFile);
        } catch (IOException e) {
            throw new CatalogException("Error opening catalog file for writing", e);
        }
    }

    @Override
    public OutputStream getOutputStream(CatalogEntry catalogEntry) {
        OutputStream entryOutput;
        try {
            File file = getFile(catalogDir, valueOf(catalogEntry));
            if (logger.isDebugEnabled()) {
                logger.debug(format("Opening file %s", file.getPath()));
            }
            entryOutput = new BufferedOutputStream(openOutputStream(file));
        } catch (IOException e) {
            throw new CatalogException("Failed opening entry output", e);
        }
        return entryOutput;
    }

    @Override
    public void close() {
        closeQuietly(output);
    }
}
