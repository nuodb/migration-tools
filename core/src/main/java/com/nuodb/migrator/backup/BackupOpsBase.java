/**
 * Copyright (c) 2015, NuoDB, Inc.
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
package com.nuodb.migrator.backup;

import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataAllOfFilters;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFiltersBase;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataInvertAcceptFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataNameEqualsFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataNameMatchesFilter;
import com.nuodb.migrator.match.Regex;
import com.nuodb.migrator.spec.MetaDataSpec;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.nuodb.migrator.match.AntRegexCompiler.INSTANCE;
import static com.nuodb.migrator.backup.XmlMetaDataHandlerBase.META_DATA_SPEC;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.*;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public abstract class BackupOpsBase implements BackupOps {

    private static final Regex BACKUP_REGEX = INSTANCE.compile("*.cat");

    private final Logger logger = getLogger(getClass());

    private String dir = DIR;
    private String file = FILE;

    @Override
    public String getDir() {
        return dir;
    }

    @Override
    public void setDir(String dir) {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Using %s directory for backup input & output", dir));
        }
        this.dir = dir == null ? DIR : dir;
    }

    @Override
    public String getFile() {
        return file;
    }

    @Override
    public void setFile(String file) {
        this.file = file;
    }

    @Override
    public String getPath() {
        return FileUtils.getFile(dir, file).getPath();
    }

    @Override
    public void setPath(String path) {
        File file = FileUtils.getFile(path == null ? EMPTY : path);
        setDir(isBackup(file) ? file.getParent() : file.getAbsolutePath());
        setFile(isBackup(file) ? file.getName() : FILE);
    }

    @Override
    public Long getLength(String name) {
        return FileUtils.getFile(dir, name).length();
    }

    private static boolean isBackup(File file) {
        return (file.exists() && file.isFile()) || BACKUP_REGEX.test(file.getName());
    }

    @Override
    public InputStream openInput(String name) {
        try {
            File file = FileUtils.getFile(getDir(), name);
            if (logger.isTraceEnabled()) {
                logger.trace(format("Opening file for reading %s", file.getPath()));
            }
            return openInputStream(file);
        } catch (IOException exception) {
            throw new BackupException("Error opening file for reading", exception);
        }
    }

    @Override
    public OutputStream openOutput(String name) {
        try {
            File file = FileUtils.getFile(getDir(), name);
            if (logger.isTraceEnabled()) {
                logger.trace(format("Opening file for writing %s", file.getPath()));
            }
            return openOutputStream(file);
        } catch (IOException exception) {
            throw new BackupException("Error opening file for writing", exception);
        }
    }

    @Override
    public Backup read() {
        return read((Map) null);
    }

    @Override
    public Backup read(Map context) {
        InputStream input = openBackupInput();
        try {
            return read(input, context);
        } finally {
            closeQuietly(input);
        }
    }

    protected InputStream openBackupInput() {
        return openInput(getFile());
    }

    @Override
    public Backup read(InputStream input) {
        return read(input, null);
    }

    public abstract Backup read(InputStream input, Map context);

    @Override
    public void write(Backup backup) {
        write(backup, (Map) null);
    }

    @Override
    public void write(Backup backup, Map context) {
        validateTableFilter(backup, context);
        OutputStream output = openBackupOutput();
        try {
            write(backup, output, context);
        } finally {
            closeQuietly(output);
        }
    }

    protected OutputStream openBackupOutput() {
        try {
            forceMkdir(FileUtils.getFile(getDir()));
        } catch (IOException exception) {
            throw new BackupException("Can't open backup directory", exception);
        }
        try {
            touch(FileUtils.getFile(getDir(), getFile()));
        } catch (IOException exception) {
            throw new BackupException("Can't open backup file", exception);
        }
        return openOutput(getFile());
    }

    @Override
    public void write(Backup backup, OutputStream output) {
        write(backup, output, null);
    }

    protected void validateTableFilter(Backup backup, Map context) {
        MetaDataSpec metaDataSpec = ((MetaDataSpec) context.get(META_DATA_SPEC));
        MetaDataFilter<Table> metaDataTablesFilter = metaDataSpec.getMetaDataFilter(MetaDataType.TABLE);
        if (!(metaDataTablesFilter == null)) {
            Collection<String> tables = new ArrayList<String>();
            for (Catalog catalog : backup.getDatabase().getCatalogs()) {
                for (Table table : catalog.getTables()) {
                    tables.add(table.getName());
                }
            }
            if (!tables.isEmpty()) {
                verifyFilter(tables, metaDataTablesFilter);
            }
        }
    }

    protected void verifyFilter(Collection<String> tables, MetaDataFilter<Table> metaDataTablesFilter) {
        Collection<MetaDataFilter<Table>> metaDataAll = ((MetaDataAllOfFilters) metaDataTablesFilter).getFilters();
        Iterator iterator = null;
        for (MetaDataFilter metaDataTableFilter : metaDataAll) {
            iterator = ((MetaDataFiltersBase) metaDataTableFilter).getFilters().iterator();
            while (iterator != null && iterator.hasNext()) {
                MetaDataFilter metaDataFilter = (MetaDataFilter) iterator.next();
                if (metaDataFilter instanceof MetaDataNameEqualsFilter) {
                    acceptFilter(tables, metaDataFilter);
                } else if (metaDataFilter instanceof MetaDataInvertAcceptFilter) {
                    MetaDataFilter<Table> filter = ((MetaDataInvertAcceptFilter) metaDataFilter).getFilter();
                    acceptFilter(tables, filter);
                } else if (metaDataFilter instanceof MetaDataNameMatchesFilter) {
                    acceptFilter(tables, metaDataFilter);
                }
            }
        }
    }

    private void acceptFilter(Collection<String> tables, MetaDataFilter<Table> filter) {
        Identifier identifier = valueOf(StringUtils.EMPTY);
        boolean accept = false;
        for (String table : tables) {
            if (filter instanceof MetaDataNameEqualsFilter) {
                identifier = ((MetaDataNameEqualsFilter) filter).getIdentifier();
                accept = ((MetaDataNameEqualsFilter) filter).accepts(table);
            }
            if (filter instanceof MetaDataNameMatchesFilter) {
                identifier = ((MetaDataNameMatchesFilter) filter).getIdentifier();
                accept = ((MetaDataNameMatchesFilter) filter).accepts(table);
            }
            if (accept)
                return;
        }
        if (!accept) {
            logWarnMessage(identifier);
        }
    }

    protected void logWarnMessage(Identifier identifier) {
        if (logger.isWarnEnabled()) {
            logger.warn(format("Table %s does not exist in the source database ", identifier.value()));
        }
    }

    @Override
    public abstract void write(Backup backup, OutputStream output, Map context);

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BackupOpsBase that = (BackupOpsBase) o;

        if (dir != null ? !dir.equals(that.dir) : that.dir != null)
            return false;
        if (file != null ? !file.equals(that.file) : that.file != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dir != null ? dir.hashCode() : 0;
        result = 31 * result + (file != null ? file.hashCode() : 0);
        return result;
    }
}