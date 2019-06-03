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
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.SelectQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;
import static java.lang.Long.parseLong;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLTableRowCountHandler extends SimpleTableRowCountHandler {

    private static final Pattern EXPLAIN_QUERY_ROW_COUNT = Pattern.compile("rows=(\\d+)");

    public PostgreSQLTableRowCountHandler(Dialect dialect, Table table, Column column, String filter,
            RowCountType rowCountType) {
        super(dialect, table, column, filter, rowCountType);
    }

    @Override
    protected TableRowCountQuery createApproxRowCountQuery() {
        Table table = getTable();
        PrimaryKey primaryKey = table.getPrimaryKey();
        Column column = getColumn();
        if (column == null && primaryKey != null && size(primaryKey.getColumns()) > 0) {
            column = get(primaryKey.getColumns(), 0);
        }
        if (column == null) {
            for (Index index : table.getIndexes()) {
                if (index.isUnique() && size(index.getColumns()) > 0) {
                    column = get(index.getColumns(), 0);
                }
            }
        }
        SelectQuery query = new SelectQuery();
        query.setDialect(getDialect());
        query.from(table);
        query.column(column != null ? column : "*");
        String filter = getFilter();
        if (filter != null) {
            query.where(filter);
        }
        return new TableRowCountQuery(table, column, filter, new ExplainQuery(query), APPROX);
    }

    @Override
    protected Long getRowCount(ResultSet resultSet, RowCountQuery rowCountQuery) throws SQLException {
        Long rowCount = null;
        switch (rowCountQuery.getRowCountType()) {
        case APPROX:
            while (rowCount == null && resultSet.next()) {
                Matcher matcher = EXPLAIN_QUERY_ROW_COUNT.matcher(resultSet.getString(1));
                if (matcher.find()) {
                    rowCount = parseLong(matcher.group(1));
                }
            }
            break;
        case EXACT:
            rowCount = resultSet.next() ? resultSet.getLong(1) : null;
            break;
        }
        return rowCount;
    }
}
