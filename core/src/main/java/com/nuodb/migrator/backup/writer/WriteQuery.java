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
package com.nuodb.migrator.backup.writer;

import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.jdbc.model.Field;
import com.nuodb.migrator.jdbc.split.QuerySplitter;

import java.util.Collection;

/**
 * Represents row set source to be dumped
 *
 * @author Sergey Bushik
 */
public class WriteQuery {

    private RowSet rowSet;
    private QuerySplitter querySplitter;
    private Collection<? extends Field> columns;

    public WriteQuery(QuerySplitter querySplitter, RowSet rowSet) {
        this.querySplitter = querySplitter;
        this.rowSet = rowSet;
    }

    public WriteQuery(QuerySplitter querySplitter, Collection<? extends Field> columns, RowSet rowSet) {
        this.querySplitter = querySplitter;
        this.columns = columns;
        this.rowSet = rowSet;
    }

    public QuerySplitter getQuerySplitter() {
        return querySplitter;
    }

    public Collection<? extends Field> getColumns() {
        return columns;
    }

    public RowSet getRowSet() {
        return rowSet;
    }
}
