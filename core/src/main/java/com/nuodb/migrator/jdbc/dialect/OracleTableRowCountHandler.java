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
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.SelectQuery;

import java.sql.SQLException;
import java.sql.Statement;

import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class OracleTableRowCountHandler extends SimpleTableRowCountHandler {

    public static final String UPDATE_STATISTICS_QUERY = "ANALYZE TABLE %s ESTIMATE STATISTICS SAMPLE 10 PERCENT";

    public static final boolean UPDATE_STATISTICS = true;

    private String updateStatisticsQuery = UPDATE_STATISTICS_QUERY;

    private boolean updateStatistics = UPDATE_STATISTICS;

    public OracleTableRowCountHandler(Dialect dialect, Table table, Column column, String filter,
            RowCountType rowCountType) {
        super(dialect, table, column, filter, rowCountType);
    }

    @Override
    protected TableRowCountQuery createApproxRowCountQuery() {
        if (getColumn() != null) {
            throw new DialectException("Approx row count query with column is not supported");
        }
        if (getFilter() != null) {
            throw new DialectException("Approx row count query with filter is not supported");
        }
        Table table = getTable();

        SelectQuery query = new SelectQuery();
        query.setDialect(getDialect());
        query.from("SYS.ALL_TABLES");
        query.column("NUM_ROWS");
        query.where("ALL_TABLES.OWNER='" + table.getSchema().getName() + "'");
        query.where("ALL_TABLES.TABLE_NAME='" + table.getName() + "'");

        return new TableRowCountQuery(table, null, null, query, APPROX);
    }

    @Override
    protected Long getRowCount(Statement statement, RowCountQuery rowCountQuery) throws SQLException {
        updateStatistics(statement, rowCountQuery);
        return super.getRowCount(statement, rowCountQuery);
    }

    protected void updateStatistics(Statement statement, RowCountQuery rowCountQuery) throws SQLException {
        if (rowCountQuery.getRowCountType() == APPROX && isUpdateStatistics()) {
            statement.execute(format(getUpdateStatisticsQuery(), getTable().getQualifiedName(getDialect())));
        }
    }

    public String getUpdateStatisticsQuery() {
        return updateStatisticsQuery;
    }

    public void setUpdateStatisticsQuery(String updateStatisticsQuery) {
        this.updateStatisticsQuery = updateStatisticsQuery;
    }

    public boolean isUpdateStatistics() {
        return updateStatistics;
    }

    public void setUpdateStatistics(boolean updateStatistics) {
        this.updateStatistics = updateStatistics;
    }
}
