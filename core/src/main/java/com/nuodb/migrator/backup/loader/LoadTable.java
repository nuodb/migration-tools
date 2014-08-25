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
package com.nuodb.migrator.backup.loader;

import com.nuodb.migrator.backup.RowSet;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.Query;
import com.nuodb.migrator.utils.ObjectUtils;

/**
 * @author Sergey Bushik
 */
public class LoadTable {

    private RowSet rowSet;
    private Table table;
    private Query query;
    private int threads;

    public LoadTable(RowSet rowSet, Table table, Query query, int threads) {
        this.rowSet = rowSet;
        this.table = table;
        this.query = query;
        this.threads = threads;
    }

    public RowSet getRowSet() {
        return rowSet;
    }

    public Table getTable() {
        return table;
    }

    public Query getQuery() {
        return query;
    }

    public int getThreads() {
        return threads;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LoadTable that = (LoadTable) o;

        if (threads != that.threads) return false;
        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        if (rowSet != null ? !rowSet.equals(that.rowSet) : that.rowSet != null) return false;
        if (table != null ? !table.equals(that.table) : that.table != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = rowSet != null ? rowSet.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (query != null ? query.hashCode() : 0);
        result = 31 * result + threads;
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
