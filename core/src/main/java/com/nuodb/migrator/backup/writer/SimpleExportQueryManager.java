/**
 * Copyright (c) 2014, NuoDB, Inc.
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
package com.nuodb.migrator.backup.writer;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import com.nuodb.migrator.backup.Chunk;
import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.backup.format.value.ValueHandle;
import com.nuodb.migrator.jdbc.session.SimpleWorkManager;
import com.nuodb.migrator.jdbc.session.Work;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Multimaps.newSetMultimap;
import static com.google.common.collect.Sets.newTreeSet;
import static com.nuodb.migrator.backup.format.value.ValueType.toAlias;

/**
 * @author Sergey Bushik
 */
public class SimpleExportQueryManager extends SimpleWorkManager implements ExportQueryManager {

    private final Map<ExportQuery, Boolean> exportQueryStartMap = newConcurrentMap();
    private final Multimap<ExportQuery, ExportQueryWork> exportQueryWorkMap = newSetMultimap(
            Maps.<ExportQuery, Collection<ExportQueryWork>>newHashMap(), new Supplier<Set<ExportQueryWork>>() {
        @Override
        public Set<ExportQueryWork> get() {
            return newTreeSet(new Comparator<ExportQueryWork>() {
                @Override
                public int compare(ExportQueryWork w1, ExportQueryWork w2) {
                    return Ints.compare(w1.getQuerySplit().getSplitIndex(), w2.getQuerySplit().getSplitIndex());
                }
            });
        }
    });

    @Override
    public void exportStart(ExportQuery exportQuery, Work work) {
        ExportQueryWork exportQueryWork = (ExportQueryWork) work;
        Boolean start = exportQueryStartMap.get(exportQuery);
        if (start == null || !start) {
            Collection<com.nuodb.migrator.backup.Column> columns = newArrayList();
            for (ValueHandle valueHandle : exportQueryWork.getValueHandleList()) {
                columns.add(new com.nuodb.migrator.backup.Column(
                        valueHandle.getName(), toAlias(valueHandle.getValueType())));
            }
            exportQuery.getRowSet().setColumns(columns);
            exportQueryStartMap.put(exportQueryWork.getExportQuery(), true);
        }
    }

    @Override
    public boolean canExport(ExportQuery exportQuery, Work work) {
        return getFailures().isEmpty();
    }

    @Override
    public void exportStart(ExportQuery exportQuery, Work work, Chunk chunk) {
    }

    @Override
    public void exportRow(ExportQuery exportQuery, Work work, Chunk chunk) {
        chunk.incrementRowCount();
    }

    @Override
    public void exportEnd(ExportQuery exportQuery, Work work, Chunk chunk) {
        RowSet rowSet = exportQuery.getRowSet();
        synchronized (rowSet) {
            rowSet.setRowCount(rowSet.getRowCount() + chunk.getRowCount());
        }
    }

    @Override
    public void exportEnd(ExportQuery exportQuery, Work work) {
        ExportQueryWork exportQueryWork = (ExportQueryWork) work;
        RowSet rowSet = exportQuery.getRowSet();
        synchronized (rowSet) {
            exportQueryWorkMap.put(exportQuery, exportQueryWork);
            final Collection<Chunk> chunks = newArrayList();
            all(exportQueryWorkMap.get(exportQuery), new Predicate<ExportQueryWork>() {
                @Override
                public boolean apply(ExportQueryWork exportQueryWork) {
                    chunks.addAll(exportQueryWork.getChunks());
                    return true;
                }
            });
            rowSet.setChunks(chunks);
        }
    }
}