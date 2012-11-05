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
package com.nuodb.migration.result.catalog;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;

import static org.apache.commons.io.FileUtils.getUserDirectoryPath;
import static org.apache.commons.io.FilenameUtils.getFullPath;
import static org.apache.commons.io.FilenameUtils.getName;

/**
 * @author Sergey Bushik
 */
public class ResultCatalogImpl implements ResultCatalog {

    private static final String CATALOG_FILE_NAME = "dump.cat";

    protected final Log log = LogFactory.getLog(getClass());

    private String path;
    private File catalogDir;
    private File catalogFile;

    public ResultCatalogImpl(String path) {
        this.path = path;
        this.catalogDir = getCatalogDir();
        this.catalogFile = getCatalogFile();
    }

    protected File getCatalogDir() {
        String dirPath = path == null ? getUserDirectoryPath() : path;
        return new File(getFullPath(dirPath));
    }

    protected File getCatalogFile() {
        String fileName = getName(path);
        if (StringUtils.isEmpty(fileName)) {
            fileName = CATALOG_FILE_NAME;
        }
        return new File(catalogDir, fileName);
    }

    public String getPath() {
        return path;
    }

    @Override
    public ResultEntryReader openReader() {
        return new ResultEntryReaderImpl(catalogDir, catalogFile);
    }

    @Override
    public ResultEntryWriter openWriter() {
        return new ResultEntryWriterImpl(catalogDir, catalogFile);
    }
}
