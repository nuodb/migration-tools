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

import com.nuodb.migrator.Migrator;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.utils.ObjectUtils;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class Backup implements HasSize {

    private Long size;
    private String version = Migrator.getVersion();
    private String format;
    private Database database = new Database();
    private Collection<RowSet> rowSets = newArrayList();

    public Backup() {
    }

    public Backup(String format) {
        this.format = format;
    }

    public Backup(String format, Database database) {
        this.format = format;
        this.database = database;
    }

    @Override
    public Long getSize() {
        return size;
    }

    @Override
    public void setSize(Long size) {
        this.size = size;
    }

    @Override
    public Long getSize(BackupOps backupOps) {
        Long size = getSize();
        if (size == null) {
            size = 0L;
            for (RowSet rowSet : getRowSets()) {
                size += rowSet.getSize(backupOps);
            }
            setSize(size);
        }
        return size;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public void addRowSet(RowSet rowSet) {
        rowSet.setBackup(this);
        rowSets.add(rowSet);
    }

    public Collection<RowSet> getRowSets() {
        return rowSets;
    }

    public void setRowSets(Collection<RowSet> rowSets) {
        if (rowSets != null) {
            for (RowSet rowSet : rowSets) {
                rowSet.setBackup(this);
            }
        }
        this.rowSets = rowSets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Backup backup = (Backup) o;

        if (database != null ? !database.equals(backup.database) : backup.database != null)
            return false;
        if (format != null ? !format.equals(backup.format) : backup.format != null)
            return false;
        if (rowSets != null ? !rowSets.equals(backup.rowSets) : backup.rowSets != null)
            return false;
        if (version != null ? !version.equals(backup.version) : backup.version != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = version != null ? version.hashCode() : 0;
        result = 31 * result + (format != null ? format.hashCode() : 0);
        result = 31 * result + (database != null ? database.hashCode() : 0);
        result = 31 * result + (rowSets != null ? rowSets.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
