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
import com.nuodb.tools.migration.jdbc.metamodel.Table;
import com.nuodb.tools.migration.jdbc.query.Query;
import com.nuodb.tools.migration.jdbc.query.SelectQuery;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.getFile;
import static org.apache.commons.io.FileUtils.openOutputStream;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.io.IOUtils.write;

/**
 * @author Sergey Bushik
 */
public class CatalogWriterImpl implements CatalogWriter {

    private static final String SELECT_QUERY_ENTRY_NAME = "table-%1$s.%2$s";
    private static final String QUERY_ENTRY_NAME = "query-%1$ty.%2$s";

    protected final Log log = LogFactory.getLog(getClass());

    private CatalogImpl catalog;
    private OutputStream catalogOutput;
    private List<String> entryNames = new ArrayList<String>();

    public CatalogWriterImpl(CatalogImpl catalog) {
        this.catalog = catalog;
    }

    protected void open() throws CatalogException {
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
        if (log.isDebugEnabled()) {
            log.debug(String.format("Dump catalog file is %1$s", catalogFile));
        }
        try {
            this.catalogOutput = openOutputStream(catalogFile);
        } catch (IOException e) {
            throw new DumpException("Error opening catalog file for writing", e);
        }
    }

    public OutputStream openEntry(Query query, String type) {
        String entryName = getEntryName(query, type);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Adding entry %1$s", entryName));
        }
        OutputStream output;
        try {
            output = new BufferedOutputStream(
                    openOutputStream(getFile(catalog.getCatalogDir(), entryName)));
        } catch (IOException e) {
            throw new DumpException("Failed opening file to output", e);
        }
        try {
            if (!entryNames.isEmpty()) {
                write(System.getProperty("line.separator"), catalogOutput);
            }
            entryNames.add(entryName);
            write(entryName, catalogOutput);
        } catch (IOException e) {
            throw new DumpException("Failed add entry in catalog", e);
        }
        return output;
    }

    protected String getEntryName(Query query, String type) {
        if (query instanceof SelectQuery) {
            List<Table> tables = ((SelectQuery) query).getTables();
            Table table = tables.get(0);
            String tableName = table.getName();
            return format(SELECT_QUERY_ENTRY_NAME, tableName, type);
        } else {
            return format(QUERY_ENTRY_NAME, new Date(), type);
        }
    }

    public void closeEntry(OutputStream output) {
        closeQuietly(output);
    }

    public void close() {
        closeQuietly(catalogOutput);
    }
}
