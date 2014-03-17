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
package com.nuodb.migrator.backup;

import com.nuodb.migrator.match.Regex;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import static com.nuodb.migrator.match.AntRegexCompiler.INSTANCE;
import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.*;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public abstract class BackupManagerBase implements BackupManager {

    private static final Regex BACKUP_REGEX = INSTANCE.compile("*.cat");

    private final Logger logger = getLogger(getClass());

    private final String dir;
    private final String backup;

    /**
     * Constructs desc manager from a full path to desc file.
     * <pre>
     * /tmp/ -> directory is /tmp, desc is /tmp/desc.cat
     * /tmp/desc.cat -> directory is /tmp, desc is /tmp/desc.cat
     * </pre>
     *
     * @param path file path to desc file
     */
    public BackupManagerBase(String path) {
        this(getFile(path == null ? EMPTY : path));
    }

    public BackupManagerBase(File file) {
        this(isBackupFile(file) ? file.getParent() : file.getAbsolutePath(),
                isBackupFile(file) ? file.getName() : BACKUP);
    }

    public BackupManagerBase(String dir, String backup) {
        this.dir = dir == null ? "." : dir;
        this.backup = backup;
        if (logger.isTraceEnabled()) {
            logger.trace(format("Using %s directory for backup input & output", dir));
        }
    }

    private static boolean isBackupFile(File file) {
        return (file.exists() && file.isFile()) || BACKUP_REGEX.test(file.getName());
    }

    @Override
    public String getDir() {
        return dir;
    }

    @Override
    public String getBackup() {
        return backup;
    }

    @Override
    public InputStream openInput(String name) {
        try {
            File file = getFile(getDir(), name);
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
            File file = getFile(getDir(), name);
            if (logger.isTraceEnabled()) {
                logger.trace(format("Opening file for writing %s", file.getPath()));
            }
            return openOutputStream(file);
        } catch (IOException exception) {
            throw new BackupException("Error opening file for writing", exception);
        }
    }

    @Override
    public Backup readBackup() {
        return readBackup((Map) null);
    }

    @Override
    public Backup readBackup(Map context) {
        InputStream input = openBackupInput();
        try {
            return readBackup(input, context);
        } finally {
            closeQuietly(input);
        }
    }

    protected InputStream openBackupInput() {
        return openInput(getBackup());
    }

    @Override
    public Backup readBackup(InputStream input) {
        return readBackup(input, null);
    }

    public abstract Backup readBackup(InputStream input, Map context);

    @Override
    public void writeBackup(Backup backup) {
        writeBackup(backup, (Map) null);
    }

    @Override
    public void writeBackup(Backup backup, Map context) {
        OutputStream output = openBackupOutput();
        try {
            writeBackup(backup, output, context);
        } finally {
            closeQuietly(output);
        }
    }

    protected OutputStream openBackupOutput() {
        try {
            forceMkdir(getFile(getDir()));
        } catch (IOException exception) {
            throw new BackupException("Can't open backup directory", exception);
        }
        try {
            touch(getFile(getDir(), getBackup()));
        } catch (IOException exception) {
            throw new BackupException("Can't open backup file", exception);
        }
        return openOutput(getBackup());
    }

    @Override
    public void writeBackup(Backup backup, OutputStream output) {
        writeBackup(backup, output, null);
    }

    @Override
    public abstract void writeBackup(Backup backup, OutputStream output, Map context);
}