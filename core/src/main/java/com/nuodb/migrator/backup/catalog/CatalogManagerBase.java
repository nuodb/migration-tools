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
package com.nuodb.migrator.backup.catalog;

import com.nuodb.migrator.match.Regex;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.nuodb.migrator.match.AntRegexCompiler.INSTANCE;
import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.*;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public abstract class CatalogManagerBase implements CatalogManager {

    private static final Regex CATALOG_REGEX = INSTANCE.compile("*.cat");

    private final Logger logger = getLogger(getClass());

    private final String directory;
    private final String catalog;

    /**
     * Constructs catalog manager from a full path to catalog file.
     * <pre>
     * /tmp/ -> directory is /tmp, catalog is /tmp/backup.cat
     * /tmp/backup.cat -> directory is /tmp, catalog is /tmp/backup.cat
     * </pre>
     *
     * @param path file path to catalog file.
     */
    public CatalogManagerBase(String path) {
        this(getFile(path == null ? EMPTY : path));
    }

    public CatalogManagerBase(File file) {
        this(isCatalogFile(file) ? file.getParent() : file.getAbsolutePath(),
                isCatalogFile(file) ? file.getName() : CATALOG);
    }

    public CatalogManagerBase(String directory, String catalog) {
        this.directory = directory == null ? "." : directory;
        this.catalog = catalog;
        if (logger.isTraceEnabled()) {
            logger.trace(format("Using %s directory for catalog output", directory));
        }
    }

    private static boolean isCatalogFile(File file) {
        return (file.exists() && file.isFile()) || CATALOG_REGEX.test(file.getName());
    }

    @Override
    public String getDirectory() {
        return directory;
    }

    @Override
    public InputStream openInputStream(String name) {
        try {
            File file = getFile(directory, name);
            if (logger.isTraceEnabled()) {
                logger.trace(format("Opening file for reading %s", file.getPath()));
            }
            return FileUtils.openInputStream(file);
        } catch (IOException exception) {
            throw new CatalogException("Error opening file for reading", exception);
        }
    }

    @Override
    public OutputStream openOutputStream(String name) {
        try {
            File file = getFile(directory, name);
            if (logger.isTraceEnabled()) {
                logger.trace(format("Opening file for writing %s", file.getPath()));
            }
            return FileUtils.openOutputStream(file);
        } catch (IOException exception) {
            throw new CatalogException("Error opening file for writing", exception);
        }
    }

    @Override
    public Catalog readCatalog() {
        InputStream inputStream = getCatalogInput();
        try {
            return readCatalog(inputStream);
        } finally {
            closeQuietly(inputStream);
        }
    }

    protected InputStream getCatalogInput() {
        return openInputStream(catalog);
    }

    protected abstract Catalog readCatalog(InputStream inputStream);

    @Override
    public void writeCatalog(Catalog catalog) {
        OutputStream outputStream = getCatalogOutput();
        try {
            writeCatalog(catalog, outputStream);
        } finally {
            closeQuietly(outputStream);
        }
    }

    protected abstract void writeCatalog(Catalog catalog, OutputStream outputStream);

    protected OutputStream getCatalogOutput() {
        try {
            forceMkdir(getFile(directory));
        } catch (IOException exception) {
            throw new CatalogException("Can't open catalog directory", exception);
        }
        try {
            touch(getFile(directory, catalog));
        } catch (IOException exception) {
            throw new CatalogException("Can't open catalog file", exception);
        }
        return openOutputStream(catalog);
    }
}