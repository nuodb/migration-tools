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
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Identifiable;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Types.BIT;
import static java.sql.Types.OTHER;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLDialect extends SimpleDialect {

    public static final JdbcTypeDesc BIT_DESC = new JdbcTypeDesc(BIT, "BIT");
    public static final JdbcTypeDesc BIT_VARYING_DESC = new JdbcTypeDesc(OTHER, "VARBIT");

    public PostgreSQLDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
    }

    @Override
    protected void initJdbcTypes() {
        super.initJdbcTypes();
        addJdbcType(PostgreSQLBitValue.INSTANCE);
        addJdbcType(PostgreSQLBitVaryingValue.INSTANCE);
    }

    /**
     * The standard says that unquoted identifiers should be normalized to upper
     * case but PostgreSQL normalizes to lower case.
     *
     * @param identifier
     *            to be normalized.
     * @param identifiable
     *            object identified by specified identifier.
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
            closeQuietly(statement);
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

    @Override
    public boolean supportsLimit() {
        return true;
    }

    /**
     * http://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
     *
     * @param statement
     * @param fetchMode
     * @throws SQLException
     */
    @Override
    public void setFetchMode(Statement statement, FetchMode fetchMode) throws SQLException {
        int fetchSize = fetchMode.getFetchSize();
        if (fetchMode.isStream() && fetchSize > 0) {
            Connection connection = statement.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            int driverVersion = metaData.getDriverMajorVersion() * 10 + metaData.getDriverMinorVersion();
            if (!connection.getAutoCommit() && (driverVersion >= 74)
                    && (statement.getResultSetType() == TYPE_FORWARD_ONLY)) {
                statement.setFetchSize(fetchSize);
            }
        } else {
            statement.setFetchSize(0);
        }
    }

    @Override
    public boolean supportsLimitParameters() {
        return true;
    }

    @Override
    public boolean supportsCatalogs() {
        return true;
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }

    @Override
    public LimitHandler createLimitHandler(String query, QueryLimit queryLimit) {
        return new PostgreSQLLimitHandler(this, query, queryLimit);
    }

    @Override
    public boolean supportsRowCount(Table table, Column column, String filter, RowCountType rowCountType) {
        return rowCountType == APPROX || rowCountType == EXACT;
    }

    @Override
    public RowCountHandler createRowCountHandler(Table table, Column column, String filter, RowCountType rowCountType) {
        return new PostgreSQLTableRowCountHandler(this, table, column, filter, rowCountType);
    }
}
