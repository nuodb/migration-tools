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
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;

/**
 * @author Sergey Bushik
 */
public class SelectQueryBuilder implements QueryBuilder<SelectQuery> {

    private Dialect dialect;
    private Table table;
    private boolean qualifyNames;
    private Collection<String> columns = Lists.newArrayList();
    private Collection<String> filters = Lists.newArrayList();

    @Override
    public SelectQuery build() {
        Collection<Column> selectQueryColumns;
        Collection<Column> tableColumns = table.getColumns();
        if (columns == null || columns.isEmpty()) {
            selectQueryColumns = tableColumns;
        } else {
            selectQueryColumns = Lists.newArrayList();
            for (String column : columns) {
                for (Column tableColumn : tableColumns) {
                    if (tableColumn.getName().equals(column)) {
                        selectQueryColumns.add(tableColumn);
                    }
                }
            }
        }
        SelectQuery selectQuery = new SelectQuery();
        Database database = table.getDatabase();
        if (dialect != null) {
            selectQuery.setDialect(dialect);
        } else if (database != null) {
            selectQuery.setDialect(database.getDialect());
        }
        selectQuery.setQualifyNames(qualifyNames);
        for (Column selectQueryColumn : selectQueryColumns) {
            selectQuery.addColumn(selectQueryColumn);
        }
        selectQuery.addTable(table);
        if (filters != null) {
            for (String filter : filters) {
                selectQuery.addFilter(filter);
            }
        }
        return selectQuery;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public boolean isQualifyNames() {
        return qualifyNames;
    }

    public void setQualifyNames(boolean qualifyNames) {
        this.qualifyNames = qualifyNames;
    }

    public void addColumn(String column) {
        this.columns.add(column);
    }

    public Collection<String> getColumns() {
        return columns;
    }

    public void setColumns(Collection<String> columns) {
        this.columns = columns;
    }

    public void addFilter(String filter) {
        this.filters.add(filter);
    }

    public Collection<String> getFilters() {
        return filters;
    }

    public void setFilters(Collection<String> filters) {
        this.filters = filters;
    }
}
