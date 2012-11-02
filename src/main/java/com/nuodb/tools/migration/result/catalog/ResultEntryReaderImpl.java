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
package com.nuodb.tools.migration.result.catalog;

import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.Set;

import static java.lang.String.valueOf;
import static org.apache.commons.io.FileUtils.getFile;
import static org.apache.commons.io.FileUtils.openInputStream;

/**
 * @author Sergey Bushik
 */
public class ResultEntryReaderImpl implements ResultEntryReader {

    protected final Log log = LogFactory.getLog(getClass());

    private File catalogDir;
    private File catalogFile;
    private InputStream input;

    public ResultEntryReaderImpl(File catalogDir, File catalogFile) {
        this.catalogDir = catalogDir;
        this.catalogFile = catalogFile;
        open();
    }

    protected void open() {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Entry catalog file is %1$s", catalogFile.getPath()));
        }
        try {
            input = openInputStream(catalogFile);
        } catch (IOException e) {
            throw new ResultCatalogException("Error opening catalog file for writing", e);
        }
    }

    @Override
    public ResultEntry[] getEntries() {
        Set<ResultEntry> entries = Sets.newHashSet();
        Scanner scanner = new Scanner(input);
        scanner.useDelimiter(System.getProperty("line.separator"));
        while (scanner.hasNext()) {
            entries.add(getEntry(scanner.next()));
        }
        scanner.close();
        return entries.toArray(new ResultEntry[entries.size()]);
    }

    protected ResultEntry getEntry(String entry) {
        int index = entry.lastIndexOf('.');
        if (index != -1) {
            return new ResultEntryImpl(entry.substring(0, index), entry.substring(index + 1));
        } else {
            throw new ResultCatalogException(String.format("Entry %s doesn't match name.type pattern", entry));
        }
    }

    @Override
    public InputStream getEntryInput(ResultEntry entry) {
        InputStream entryInput;
        try {
            entryInput = openInputStream(getFile(catalogDir, valueOf(entry)));
        } catch (IOException e) {
            throw new ResultCatalogException("Failed opening entry input", e);
        }
        return entryInput;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(input);
    }

    public static void main(String[] args) {
        ResultCatalog catalog = new ResultCatalogImpl("/tmp/test/dump.cat");
        ResultEntryReader reader = catalog.openReader();
        for (ResultEntry entry : reader.getEntries()) {
            System.out.println(entry.getName() + "." + entry.getType());
        }
    }
}
