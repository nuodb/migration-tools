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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.SelectQuery;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleTableRowCountHandler extends SimpleRowCountHandler implements TableRowCountHandler {

    private Table table;
    private Column column;
    private String filter;

    public SimpleTableRowCountHandler(Dialect dialect, Table table, Column column, String filter,
            RowCountType rowCountType) {
        super(dialect, rowCountType);
        this.table = table;
        this.column = column;
        this.filter = filter;
    }

    @Override
    public TableRowCountQuery getRowCountQuery() {
        return (TableRowCountQuery) super.getRowCountQuery();
    }

    @Override
    protected TableRowCountQuery createExactRowCountQuery() {
        SelectQuery query = new SelectQuery();
        query.setQualifyNames(true);
        query.setDialect(getDialect());
        query.from(getTable());
        PrimaryKey primaryKey = getTable().getPrimaryKey();
        Column column = getColumn();
        if (column == null && primaryKey != null && size(primaryKey.getColumns()) > 0) {
            column = get(primaryKey.getColumns(), 0);
        }
        query.column("COUNT(" + (column != null ? column.getName(getDialect()) : "*") + ")");
        if (getFilter() != null) {
            query.where(getFilter());
        }
        return new TableRowCountQuery(getTable(), column, getFilter(), query, EXACT);
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public Column getColumn() {
        return column;
    }

    @Override
    public String getFilter() {
        return filter;
    }
}
