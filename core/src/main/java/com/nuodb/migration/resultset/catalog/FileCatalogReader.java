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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

import static java.lang.String.valueOf;
import static org.apache.commons.io.FileUtils.getFile;
import static org.apache.commons.io.FileUtils.openInputStream;
import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * @author Sergey Bushik
 */
public class FileCatalogReader implements CatalogReader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private File catalogDir;
    private File catalogFile;

    public FileCatalogReader(File catalogDir, File catalogFile) {
        this.catalogDir = catalogDir;
        this.catalogFile = catalogFile;
    }

    @Override
    public CatalogEntry[] getEntries() {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Entry catalog file is %1$s", catalogFile.getPath()));
        }
        InputStream input = getCatalogInput();
        try {
            return readEntries(input);
        } finally {
            closeQuietly(input);
        }
    }

    protected InputStream getCatalogInput() {
        try {
            return openInputStream(catalogFile);
        } catch (IOException exception) {
            throw new CatalogException("Error opening catalog file for reading", exception);
        }
    }

    protected CatalogEntry[] readEntries(InputStream entriesInput) {
        List<CatalogEntry> entries = Lists.newArrayList();
        Scanner scanner = new Scanner(entriesInput);
        scanner.useDelimiter(System.getProperty("line.separator"));
        while (scanner.hasNext()) {
            entries.add(getEntry(scanner.next()));
        }
        scanner.close();
        return entries.toArray(new CatalogEntry[entries.size()]);
    }

    protected CatalogEntry getEntry(String entry) {
        int index = entry.lastIndexOf('.');
        if (index != -1) {
            return new CatalogEntry(entry.substring(0, index), entry.substring(index + 1));
        } else {
            throw new CatalogException(String.format("Entry %s doesn't match pattern", entry));
        }
    }

    @Override
    public InputStream getEntryInput(CatalogEntry entry) {
        InputStream entryInput;
        try {
            File file = getFile(catalogDir, valueOf(entry));
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Opening file %s", file.getPath()));
            }
            entryInput = openInputStream(file);
        } catch (IOException e) {
            throw new CatalogException("Failed opening entry input", e);
        }
        return entryInput;
    }

    @Override
    public void close() {
    }
}
