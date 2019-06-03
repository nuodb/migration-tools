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
package com.nuodb.migrator.backup.format.value;

import com.nuodb.migrator.backup.BackupOps;
import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.Input;

import java.util.Iterator;
import java.util.Map;

import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * @author Sergey Bushik
 */
public class RowReaders {

    public static RowReader newSequentialRowReader(RowSet rowSet, BackupOps backupOps, FormatFactory formatFactory,
            Map<String, Object> formatAttributes) {
        return new SequentialRowReader(rowSet, backupOps, formatFactory, formatAttributes);
    }

    public static RowReader newSynchronizedRowReader(RowReader rowReader) {
        return new SynchronizedRowReader(rowReader);
    }

    public static RowReader newSynchronizedRowReader(RowReader rowReader, Object mutex) {
        return new SynchronizedRowReader(rowReader, mutex);
    }

    static class SequentialRowReader implements RowReader {

        private final RowSet rowSet;
        private final BackupOps backupOps;
        private final FormatFactory formatFactory;
        private final Map<String, Object> formatAttributes;

        private Iterator<Chunk> chunks;
        private Chunk chunk;
        private Input input;
        private Row row;
        private volatile long number;

        SequentialRowReader(RowSet rowSet, BackupOps backupOps, FormatFactory formatFactory,
                Map<String, Object> formatAttributes) {
            this.rowSet = rowSet;
            this.backupOps = backupOps;
            this.formatFactory = formatFactory;
            this.formatAttributes = formatAttributes;
        }

        @Override
        public Row readRow() {
            initChunk();
            initInput();
            initRowValues();
            return row;
        }

        @Override
        public void close() {
            if (input != null) {
                closeQuietly(input);
                input = null;
            }
        }

        protected void initChunk() {
            if (chunks == null) {
                chunks = rowSet.getChunks().iterator();
            }
            if (chunks.hasNext() && chunk == null) {
                chunk = chunks.next();
            }
        }

        protected void initInput() {
            if (chunk != null && input == null) {
                input = formatFactory.createInput(rowSet.getBackup().getFormat(), formatAttributes);
                input.setInputStream(backupOps.openInput(chunk.getName()));
                input.setRowSet(rowSet);
                input.init();
                input.readStart();
                number = 0;
            }
        }

        protected void initRowValues() {
            Value[] values = null;
            if (input != null) {
                try {
                    values = input.readValues();
                } finally {
                    if (values == null) {
                        input.readEnd();
                        input.close();
                        input = null;
                        chunk = null;
                    }
                }
            }
            row = values != null ? new Row(chunk, values, number++) : null;
        }
    }

    static class SynchronizedRowReader implements RowReader {

        private final RowReader rowReader;
        private final Object mutex;

        SynchronizedRowReader(RowReader rowReader) {
            this.rowReader = rowReader;
            this.mutex = this;
        }

        SynchronizedRowReader(RowReader rowReader, Object mutex) {
            this.rowReader = rowReader;
            this.mutex = mutex;
        }

        @Override
        public Row readRow() {
            synchronized (mutex) {
                return rowReader.readRow();
            }
        }

        @Override
        public void close() {
            synchronized (mutex) {
                rowReader.close();
            }
        }
    }
}
