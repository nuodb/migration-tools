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
package com.nuodb.tools.migration.dump.catalog;

import com.nuodb.tools.migration.dump.DumpException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static org.apache.commons.io.FileUtils.getFile;
import static org.apache.commons.io.FileUtils.openOutputStream;
import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * @author Sergey Bushik
 */
public class QueryEntryWriterImpl implements QueryEntryWriter {
    protected final Log log = LogFactory.getLog(getClass());

    private EntryCatalogImpl catalog;
    private OutputStream catalogOutput;
    private List<QueryEntry> catalogEntries = new ArrayList<QueryEntry>();
    private Map<QueryEntry, OutputStream> catalogEntriesOutputs = new HashMap<QueryEntry, OutputStream>();

    public QueryEntryWriterImpl(EntryCatalogImpl catalog) {
        this.catalog = catalog;
    }

    protected void open() throws EntryCatalogException {
        File catalogDir = catalog.getCatalogDir();
        try {
            FileUtils.forceMkdir(catalogDir);
        } catch (IOException e) {
            throw new DumpException("Can't open dump catalog dir", e);
        }
        File catalogFile = catalog.getCatalogFile();
        try {
            FileUtils.touch(catalogFile);
        } catch (IOException e) {
            throw new DumpException("Can't open dump catalog file", e);
        }
        try {
            this.catalogOutput = openOutputStream(catalogFile);
        } catch (IOException e) {
            throw new DumpException("Error opening catalog file for writing", e);
        }
    }

    public OutputStream write(QueryEntry entry) {
        String entryName = getEntryName(entry);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Adding entry %1$s", entryName));
        }
        OutputStream catalogEntryOutput;
        try {
            catalogEntryOutput = new BufferedOutputStream(
                    openOutputStream(getFile(catalog.getCatalogDir(), entryName)));
        } catch (IOException e) {
            throw new DumpException("Failed opening file to output", e);
        }
        try {
            if (!catalogEntries.isEmpty()) {
                IOUtils.write(System.getProperty("line.separator"), catalogOutput);
            }
            catalogEntries.add(entry);
            catalogEntriesOutputs.put(entry, catalogEntryOutput);
            IOUtils.write(entryName, catalogOutput);
        } catch (IOException e) {
            throw new DumpException("Failed add entry in catalog", e);
        }
        return catalogEntryOutput;
    }

    protected String getEntryName(QueryEntry entry) {
        StringBuilder entryName = new StringBuilder();
        entryName.append(entry.getName());
        entryName.append('.');
        entryName.append(catalog.getType());
        return entryName.toString();
    }

    public void close(QueryEntry entry) {
        closeQuietly(catalogEntriesOutputs.remove(entry));
    }

    public void close() {
        Iterator<OutputStream> iterator = catalogEntriesOutputs.values().iterator();
        while (iterator.hasNext()) {
            closeQuietly(iterator.next());
            iterator.remove();
        }
        closeQuietly(catalogOutput);
    }
}
