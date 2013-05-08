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
package com.nuodb.migrator.jdbc.query;

import com.google.common.collect.Lists;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;
import java.util.Iterator;

import static com.nuodb.migrator.jdbc.query.QueryUtils.where;

/**
 * @author Sergey Bushik
 */
public class SelectQuery implements Query {

    private Dialect dialect;
    private boolean qualifyNames;
    private Collection<Table> tables = Lists.newArrayList();
    private Collection<Object> columns = Lists.newArrayList();
    private Collection<String> filters = Lists.newArrayList();

    public void addColumn(Object column) {
        columns.add(column);
    }

    public void addTable(Table table) {
        tables.add(table);
    }

    public void addFilter(String filter) {
        filters.add(filter);
    }

    @Override
    public String toQuery() {
        return buildQuery().toString();
    }

    protected StringBuilder buildQuery() {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        addColumns(query);
        query.append(" FROM ");
        addTables(query);
        addFilters(query);
        return query;
    }

    protected void addColumns(StringBuilder query) {
        for (Iterator<Object> iterator = columns.iterator(); iterator.hasNext(); ) {
            addColumn(query, iterator.next());
            if (iterator.hasNext()) {
                query.append(", ");
            }
        }
    }

    protected void addColumn(StringBuilder query, Object column) {
        if (column instanceof Column) {
            query.append(((Column) column).getName(dialect));
        } else {
            query.append(column);
        }
    }

    protected void addTables(StringBuilder query) {
        for (Iterator<Table> iterator = tables.iterator(); iterator.hasNext(); ) {
            addTable(query, iterator.next());
            if (iterator.hasNext()) {
                query.append(", ");
            }
        }
    }

    protected void addFilters(StringBuilder query) {
        where(query, filters, "AND");
    }

    protected void addTable(StringBuilder query, Table table) {
        query.append(qualifyNames ? table.getQualifiedName(dialect) : table.getName(dialect));
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public boolean isQualifyNames() {
        return qualifyNames;
    }

    public void setQualifyNames(boolean qualifyNames) {
        this.qualifyNames = qualifyNames;
    }

    public Collection<Table> getTables() {
        return tables;
    }

    public Collection<Object> getColumns() {
        return columns;
    }

    public Collection<String> getFilters() {
        return filters;
    }

    @Override
    public String toString() {
        return toQuery();
    }
}
