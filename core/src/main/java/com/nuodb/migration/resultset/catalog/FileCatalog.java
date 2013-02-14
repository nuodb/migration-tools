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

import com.nuodb.migration.match.AntRegexCompiler;
import com.nuodb.migration.match.Regex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * @author Sergey Bushik
 */
public class FileCatalog implements Catalog {

    private static Regex CATALOG_FILE_REGEX = AntRegexCompiler.INSTANCE.compile("*.cat");
    private static final String CATALOG_FILE_NAME = "dump.cat";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private String path;
    private File pathFile;
    private File catalogDir;
    private File catalogFile;

    public FileCatalog(String path) {
        this.path = path;
        this.pathFile = getPathFile();
        this.catalogDir = getCatalogDir();
        this.catalogFile = getCatalogFile();
    }

    protected File getPathFile() {
        return new File(path == null ? EMPTY : path).getAbsoluteFile();
    }

    protected File getCatalogDir() {
        return isPathCatalogFile(pathFile) ? pathFile.getParentFile() : pathFile;
    }

    protected boolean isPathCatalogFile(File pathFile) {
        return ((pathFile.exists() && pathFile.isFile()) || isCatalogFileLike(pathFile));
    }

    protected boolean isCatalogFileLike(File pathFile) {
        return CATALOG_FILE_REGEX.test(pathFile.getName());
    }

    protected File getCatalogFile() {
        String catalogFile = isPathCatalogFile(pathFile) ? pathFile.getName() : CATALOG_FILE_NAME;
        return new File(catalogDir, catalogFile);
    }

    public String getPath() {
        return path;
    }

    @Override
    public CatalogReader getCatalogReader() {
        return new FileCatalogReader(catalogDir, catalogFile);
    }

    @Override
    public CatalogWriter getCatalogWriter() {
        return new FileCatalogWriter(catalogDir, catalogFile);
    }
}
