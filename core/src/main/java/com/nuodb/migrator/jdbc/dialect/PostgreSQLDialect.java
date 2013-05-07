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

import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.nuodb.migrator.jdbc.JdbcUtils.close;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;
import static java.lang.Long.parseLong;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLDialect extends SimpleDialect {

    private static final Pattern EXPLAIN_QUERY_ROW_COUNT = Pattern.compile("rows=(\\d+)");

    public PostgreSQLDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
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
        rowCountQuery.setTable(table);
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
                while (rowCountValue == null && rowCount.next()) {
                    Matcher matcher = EXPLAIN_QUERY_ROW_COUNT.matcher(rowCount.getString(1));
                    if (matcher.find()) {
                        rowCountValue = new RowCountValue(rowCountQuery, parseLong(matcher.group(1)));
                    }
                }
                break;
            case EXACT:
                rowCountValue = rowCount.next() ? new RowCountValue(rowCountQuery, rowCount.getLong(1)) : null;
                break;
        }
        return rowCountValue;
    }

    /**
     * The standard says that unquoted identifiers should be normalized to upper case but PostgreSQL normalizes to lower
     * case.
     *
     * @param identifier   to be normalized.
     * @param identifiable
     * @return boolean indicating whether quoting is required.
     */
    @Override
    public boolean isQuotingIdentifier(String identifier, Identifiable identifiable) {
        boolean quote = false;
        for (int i = 0, length = identifier.length(); i < length; i++) {
            if (Character.isUpperCase(identifier.charAt(i))) {
                quote = true;
                break;
            }
        }
        return quote || super.isQuotingIdentifier(identifier, identifiable);
    }

    @Override
    public boolean supportsSessionTimeZone() {
        return true;
    }

    @Override
    public void setSessionTimeZone(Connection connection, TimeZone timeZone) throws SQLException {
        Statement statement = connection.createStatement();
        try {
            String timeZoneAsValue = timeZone != null ? timeZoneAsValue(timeZone) : "LOCAL";
            statement.execute("SET TIME ZONE " + timeZoneAsValue);
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

        StringBuilder value = new StringBuilder(32);
        value.append("INTERVAL ");
        value.append("'");
        value.append(rawOffset >= 0 ? '+' : '-');
        value.append(zeros.substring(0, zeros.length() - hoursOffset.length()));
        value.append(hoursOffset);
        value.append(':');
        value.append(zeros.substring(0, zeros.length() - minutesOffset.length()));
        value.append(minutesOffset);
        value.append("'");
        value.append(" HOUR TO MINUTE");
        return value.toString();
    }

    @Override
    public String getCascadeConstraints() {
        return "CASCADE";
    }

    @Override
    public boolean supportsDropConstraints() {
        return true;
    }

    @Override
    public boolean supportsIfExistsBeforeDropTable() {
        return true;
    }

    @Override
    public boolean supportsDropSequenceIfExists() {
        return true;
    }
}
