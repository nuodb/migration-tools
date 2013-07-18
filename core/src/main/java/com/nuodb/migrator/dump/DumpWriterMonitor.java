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
package com.nuodb.migrator.dump;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import com.nuodb.migrator.backup.catalog.Chunk;
import com.nuodb.migrator.backup.catalog.Column;
import com.nuodb.migrator.backup.catalog.RowSet;
import com.nuodb.migrator.backup.format.value.ValueHandle;
import org.slf4j.Logger;

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
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "SynchronizationOnLocalVariableOrMethodParameter"})
class DumpWriterMonitor implements DumpQueryMonitor {

    private final transient Logger logger = getLogger(getClass());

    private final Map<DumpTask, Exception> errorMap = newConcurrentMap();
    private final Map<QueryHandle, Boolean> queryHandleInitMap = newConcurrentMap();
    private Multimap<QueryHandle, DumpQuery> queryHandleDumpQueryMap = newSetMultimap(
            Maps.<QueryHandle, Collection<DumpQuery>>newHashMap(), new Supplier<Set<DumpQuery>>() {
        @Override
        public Set<DumpQuery> get() {
            return newTreeSet(new Comparator<DumpQuery>() {
                @Override
                public int compare(DumpQuery o1, DumpQuery o2) {
                    return Ints.compare(o1.getQuerySplit().getSplitIndex(), o2.getQuerySplit().getSplitIndex());
                }
            });
        }
    });

    @Override
    public void executeStart(DumpQuery dumpQuery) {
        Boolean queryDescInit = queryHandleInitMap.get(dumpQuery.getQueryHandle());
        if (queryDescInit == null || !queryDescInit) {
            Collection<Column> columns = newArrayList();
            for (ValueHandle valueHandle : dumpQuery.getValueHandleList()) {
                columns.add(new Column(valueHandle.getName(),
                        toAlias(valueHandle.getValueType())));
            }
            dumpQuery.getRowSet().setColumns(columns);
            queryHandleInitMap.put(dumpQuery.getQueryHandle(), true);
        }
    }

    @Override
    public boolean canWrite(DumpQuery dumpQuery) {
        return errorMap.isEmpty();
    }

    @Override
    public void writeStart(DumpQuery dumpQuery, Chunk chunk) {
    }

    @Override
    public void writeValues(DumpQuery dumpQuery, Chunk chunk) {
        chunk.incrementRowCount();
    }

    @Override
    public void writeEnd(DumpQuery dumpQuery, Chunk chunk) {
        final RowSet rowSet = dumpQuery.getRowSet();
        synchronized (rowSet) {
            rowSet.setRowCount(rowSet.getRowCount() + chunk.getRowCount());
        }
    }

    @Override
    public void executeEnd(DumpQuery dumpQuery) {
        final RowSet rowSet = dumpQuery.getRowSet();
        synchronized (rowSet) {
            queryHandleDumpQueryMap.put(dumpQuery.getQueryHandle(), dumpQuery);
            final Collection<Chunk> chunks = newArrayList();
            all(queryHandleDumpQueryMap.get(dumpQuery.getQueryHandle()), new Predicate<DumpQuery>() {
                @Override
                public boolean apply(DumpQuery dumpQuery) {
                    chunks.addAll(dumpQuery.getChunks());
                    return true;
                }
            });
            rowSet.setChunks(chunks);
        }
    }

    @Override
    public void error(DumpTask dumpTask, Exception exception) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Dump write error reported", exception);
        }
        errorMap.put(dumpTask, exception);
    }

    @Override
    public Map<DumpTask, Exception> getErrors() {
        return errorMap;
    }
}