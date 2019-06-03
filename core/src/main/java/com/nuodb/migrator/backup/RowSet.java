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

import com.nuodb.migrator.backup.format.value.ValueType;
import com.nuodb.migrator.utils.ObjectUtils;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class RowSet implements HasSize {

    private Long size;
    private String name;
    private AtomicLong rowCount = new AtomicLong();
    private String type;
    private Collection<Column> columns = newArrayList();
    private Collection<Chunk> chunks = newArrayList();
    private transient Backup backup;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
            for (Chunk chunk : getChunks()) {
                size += chunk.getSize(backupOps);
            }
            setSize(size);
        }
        return size;
    }

    public long getRowCount() {
        return rowCount.get();
    }

    public void setRowCount(long rowCount) {
        this.rowCount.set(rowCount);
    }

    public void incrementRowCount() {
        rowCount.incrementAndGet();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void addColumn(String name, ValueType valueType) {
        addColumn(new Column(name, valueType));
    }

    public void addColumn(Column column) {
        column.setRowSet(this);
        columns.add(column);
    }

    public Collection<Column> getColumns() {
        return columns;
    }

    public void setColumns(Collection<Column> columns) {
        for (Column column : columns) {
            column.setRowSet(this);
        }
        this.columns = columns;
    }

    public void addChunk(Chunk chunk) {
        chunk.setRowSet(this);
        chunks.add(chunk);
        rowCount.addAndGet(chunk.getRowCount());
    }

    public Collection<Chunk> getChunks() {
        return chunks;
    }

    public void setChunks(Collection<Chunk> chunks) {
        long rowCount = 0;
        for (Chunk chunk : chunks) {
            chunk.setRowSet(this);
            rowCount += chunk.getRowCount();
        }
        this.chunks = chunks;
        this.rowCount.set(rowCount);
    }

    public Backup getBackup() {
        return backup;
    }

    public void setBackup(Backup backup) {
        this.backup = backup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RowSet rowSet = (RowSet) o;

        if (getRowCount() != rowSet.getRowCount())
            return false;
        if (chunks != null ? !chunks.equals(rowSet.chunks) : rowSet.chunks != null)
            return false;
        if (columns != null ? !columns.equals(rowSet.columns) : rowSet.columns != null)
            return false;
        if (name != null ? !name.equals(rowSet.name) : rowSet.name != null)
            return false;
        if (type != null ? !type.equals(rowSet.type) : rowSet.type != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        long rowCount = getRowCount();
        result = 31 * result + (int) (rowCount ^ (rowCount >>> 32));
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        result = 31 * result + (chunks != null ? chunks.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
