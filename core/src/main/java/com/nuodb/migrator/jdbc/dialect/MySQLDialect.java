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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;

import java.sql.*;
import java.util.TimeZone;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.dialect.MySQLSelectQuery.IndexHint;
import static com.nuodb.migrator.jdbc.dialect.MySQLSelectQuery.IndexUsage;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
public class MySQLDialect extends SimpleDialect {

    public MySQLDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
    }

    @Override
    protected void initJdbcTypes() {
        super.initJdbcTypes();

        addJdbcType(MySQLIntUnsignedType.INSTANCE);
        addJdbcType(MySQLBigIntUnsignedType.INSTANCE);

        addJdbcTypeDescAlias(Types.LONGVARCHAR, "TINYTEXT", Types.CLOB);
        addJdbcTypeDescAlias(Types.LONGVARCHAR, "TEXT", Types.CLOB);
        addJdbcTypeDescAlias(Types.LONGVARCHAR, "MEDIUMTEXT", Types.CLOB);
        addJdbcTypeDescAlias(Types.LONGVARCHAR, "LONGTEXT", Types.CLOB);

        addJdbcTypeDescAlias(Types.LONGVARBINARY, "TINYBLOB", Types.BLOB);
        addJdbcTypeDescAlias(Types.LONGVARBINARY, "BLOB", Types.BLOB);
        addJdbcTypeDescAlias(Types.LONGVARBINARY, "MEDIUMBLOB", Types.BLOB);
        addJdbcTypeDescAlias(Types.LONGVARBINARY, "LONGBLOB", Types.BLOB);
    }

    @Override
    public String getDropForeignKey() {
        return "DROP FOREIGN KEY";
    }

    @Override
    public String openQuote() {
        return valueOf('`');
    }

    @Override
    public String closeQuote() {
        return valueOf('`');
    }

    @Override
    public boolean supportsSessionTimeZone() {
        return true;
    }

    @Override
    public boolean supportsColumnCheck() {
        return false;
    }

    @Override
    public String getTableComment(String comment) {
        return " COMMENT='" + comment + "'";
    }

    @Override
    public String getColumnComment(String comment) {
        return " COMMENT '" + comment + "'";
    }

    @Override
    public void setSessionTimeZone(Connection connection, TimeZone timeZone) throws SQLException {
        Statement statement = connection.createStatement();
        try {
            String timeZoneAsValue = timeZone != null ? timeZoneAsValue(timeZone) : "SYSTEM";
            statement.execute("SET @@SESSION.TIME_ZONE = '" + timeZoneAsValue + "'");
        } finally {
            close(statement);
        }
    }

    protected String timeZoneAsValue(TimeZone timeZone) {
        int rawOffset = timeZone.getRawOffset();
        int dstSavings = timeZone.getDSTSavings();
        int absOffset = Math.abs(rawOffset + dstSavings);
        String zeros = "00";
        String hoursOffset = Integer.toString(absOffset / 3600000);
        String minutesOffset = Integer.toString((absOffset % 3600000) / 60000);

        StringBuilder value = new StringBuilder(6);
        value.append(rawOffset >= 0 ? '+' : '-');
        value.append(zeros.substring(0, zeros.length() - hoursOffset.length()));
        value.append(hoursOffset);
        value.append(':');
        value.append(zeros.substring(0, zeros.length() - minutesOffset.length()));
        value.append(minutesOffset);
        return value.toString();
    }

    @Override
    protected RowCountQuery createRowCountExactQuery(Table table) {
        MySQLSelectQuery query = new MySQLSelectQuery();
        query.setDialect(this);
        query.addTable(table);
        for (Index index : table.getIndexes()) {
            if (index.isPrimary()) {
                query.setIndexHint(new IndexHint(IndexUsage.FORCE, index));
                break;
            }
        }
        query.addColumn("COUNT(*)");
        RowCountQuery rowCountQuery = new RowCountQuery();
        rowCountQuery.setRowCountType(EXACT);
        rowCountQuery.setQuery(query);
        return rowCountQuery;
    }

    @Override
    protected RowCountQuery createRowCountApproxQuery(Table table) {
        PrimaryKey primaryKey = table.getPrimaryKey();
        Column column = null;
        if (primaryKey != null && size(primaryKey.getColumns()) > 0) {
            column = get(primaryKey.getColumns(), 0);
        }
        if (column == null) {
            for (Index index : table.getIndexes()) {
                if (index.isUnique() && size(index.getColumns()) > 0) {
                    column = get(index.getColumns(), 0);
                }
            }
        }
        SelectQuery selectQuery = new SelectQuery();
        selectQuery.setDialect(this);
        selectQuery.addTable(table);
        selectQuery.addColumn(column != null ? column : "*");

        RowCountQuery rowCountQuery = new RowCountQuery();
        rowCountQuery.setColumn(column);
        rowCountQuery.setRowCountType(APPROX);
        rowCountQuery.setQuery(new ExplainQuery(selectQuery));
        return rowCountQuery;
    }

    @Override
    protected RowCountValue extractRowCountValue(ResultSet rowCount, RowCountQuery rowCountQuery) throws SQLException {
        RowCountValue rowCountValue = null;
        switch (rowCountQuery.getRowCountType()) {
            case APPROX:
                rowCountValue = new RowCountValue(rowCountQuery, rowCount.getLong("ROWS"));
                break;
            case EXACT:
                rowCountValue = new RowCountValue(rowCountQuery, rowCount.getLong(1));
                break;
        }
        return rowCountValue;
    }

    /**
     * Forces driver to stream ResultSet http://goo.gl/kl1Nr
     *
     * @param statement to stream ResultSet
     * @throws SQLException
     */
    @Override
    public void setStreamResults(Statement statement, boolean streamResults) throws SQLException {
        statement.setFetchSize(streamResults ? Integer.MIN_VALUE : 0);
    }

    @Override
    public boolean supportsIfExistsBeforeDropTable() {
        return true;
    }
}
