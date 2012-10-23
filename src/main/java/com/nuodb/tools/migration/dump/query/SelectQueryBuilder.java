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
package com.nuodb.tools.migration.dump.query;

import com.nuodb.tools.migration.jdbc.metamodel.*;
import org.hibernate.dialect.Dialect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Sergey Bushik
 */
public class SelectQueryBuilder {

    private Dialect dialect;
    private Table table;
    private List<String> columns = new ArrayList<String>();
    private List<String> filters = new ArrayList<String>();

    public SelectQuery build() {
        Collection<Column> selectQueryColumns;
        Collection<Column> tableColumns = table.listColumns();
        if (columns == null || columns.isEmpty()) {
            selectQueryColumns = tableColumns;
        } else {
            selectQueryColumns = new ArrayList<Column>();
            for (String column : columns) {
                for (Column tableColumn : tableColumns) {
                    if (tableColumn.getObjectName().value().equals(column)) {
                        selectQueryColumns.add(tableColumn);
                    }
                }
            }
        }
        SelectQuery selectQuery = new SelectQuery();
        if (dialect != null) {
            selectQuery.setDialect(dialect);
        } else {
            selectQuery.setDialect(table.getDatabase().getDialect());
        }
        selectQuery.setQualify(true);
        for (Column selectQueryColumn : selectQueryColumns) {
            selectQuery.addColumn(selectQueryColumn);
        }
        selectQuery.addTable(table);
        if (filters != null) {
            for (String filter : filters) {
                selectQuery.addCondition(filter);
            }
        }
        return selectQuery;
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

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void addColumn(String column) {
        this.columns.add(column);
    }

    public List<String> getFilters() {
        return filters;
    }

    public void setFilters(List<String> filters) {
        this.filters = filters;
    }

    public void addFilter(String filter) {
        this.filters.add(filter);
    }
}
