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
package com.nuodb.tools.migration.dump;

import com.nuodb.tools.migration.dump.query.Query;
import com.nuodb.tools.migration.dump.query.SelectQuery;
import com.nuodb.tools.migration.jdbc.metamodel.Table;
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
import static org.apache.commons.io.FileUtils.*;
import static org.apache.commons.io.FilenameUtils.getFullPath;
import static org.apache.commons.io.FilenameUtils.getName;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.io.IOUtils.write;

/**
 * @author Sergey Bushik
 */
public class DumpCatalog {

    protected final Log log = LogFactory.getLog(getClass());

    private static final String CATALOG_FILE_NAME = "dump-%1$ty-%1$tm-%1$te-%1$tH-%1$tM.cat";
    private static final String SELECT_QUERY_ENTRY_NAME = "table-%1$s-%2$ty-%2$tm-%2$te-%2$tH-%2$tM.%3$s";
    private static final String QUERY_ENTRY_NAME = "query-%1$ty-%1$tm-%1$te-%1$tH-%1$tM.%2$s";

    private String path;
    private File catalogDir;
    private File catalogFile;
    private OutputStream catalogFileOutput;
    private List<String> entryNames = new ArrayList<String>();

    public DumpCatalog(String path) {
        this.path = path;
    }

    public void open() {
        try {
            this.catalogDir = makeCatalogDir();
        } catch (IOException e) {
            throw new DumpException("Can't create dump catalog dir", e);
        }
        try {
            this.catalogFile = makeCatalogFile();
        } catch (IOException e) {
            throw new DumpException("Can't create dump catalog file", e);
        }
        if (log.isDebugEnabled()) {
            log.debug(format("Dump catalog file is %1$s", getCatalogFile().getPath()));
        }
        try {
            this.catalogFileOutput = openOutputStream(getCatalogFile());
        } catch (IOException e) {
            throw new DumpException("Error opening catalog file for writing", e);
        }
    }

    protected File makeCatalogDir() throws IOException {
        String dirPath = path == null ? getUserDirectoryPath() : path;
        File dir = new File(getFullPath(dirPath));
        FileUtils.forceMkdir(dir);
        return dir;
    }

    protected File makeCatalogFile() throws IOException {
        String fileName = getName(path);
        if (fileName.isEmpty()) {
            fileName = format(CATALOG_FILE_NAME, new Date());
        }
        File catalogFile = getFile(getCatalogDir(), fileName);
        touch(catalogFile);
        return catalogFile;
    }

    public OutputStream addEntry(Query query, String type) {
        String entryName = getEntryName(query, type);
        OutputStream output;
        try {
            output = new BufferedOutputStream(
                    openOutputStream(getFile(getCatalogDir(), entryName)));
        } catch (IOException e) {
            throw new DumpException("Failed opening file to output", e);
        }
        try {
            if (entryNames.isEmpty()) {
                write(System.getProperty("line.separator"), getCatalogFileOutput());
            }
            entryNames.add(entryName);
            write(entryName, getCatalogFileOutput());
        } catch (IOException e) {
            throw new DumpException("Failed add entry in catalog", e);
        }
        return output;
    }

    protected String getEntryName(Query query, String type) {
        if (query instanceof SelectQuery) {
            List<Table> tables = ((SelectQuery) query).getTables();
            Table table = tables.get(0);
            String tableName = table.getName().value();
            return format(SELECT_QUERY_ENTRY_NAME, tableName, new Date(), type);
        } else {
            return format(QUERY_ENTRY_NAME, new Date(), type);
        }
    }

    public void closeEntry(OutputStream output) {
        closeQuietly(output);
    }

    public void close() {
        closeQuietly(catalogFileOutput);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    protected File getCatalogDir() {
        return catalogDir;
    }

    protected File getCatalogFile() {
        return catalogFile;
    }

    protected OutputStream getCatalogFileOutput() {
        return catalogFileOutput;
    }
}
