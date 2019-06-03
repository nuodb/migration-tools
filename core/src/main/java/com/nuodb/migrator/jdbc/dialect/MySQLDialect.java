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
import com.nuodb.migrator.jdbc.metadata.ColumnTrigger;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.NUODB;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newSize;
import static com.nuodb.migrator.utils.Priority.HIGH;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.String.valueOf;
import static java.sql.Types.*;

/**
 * @author Sergey Bushik
 */
public class MySQLDialect extends SimpleDialect {

    public MySQLDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
    }

    @Override
    public String getIdentityColumn(String sequence) {
        return "AUTO_INCREMENT";
    }

    @Override
    protected void initJdbcTypes() {
        super.initJdbcTypes();

        addJdbcType(MySQLYearValue.INSTANCE);
        addJdbcType(MySQLSmallIntValue.INSTANCE);
        addJdbcType(MySQLIntegerValue.INSTANCE);
        addJdbcType(MySQLBigIntValue.INSTANCE);
        addJdbcType(MySQLSmallIntUnsigned.INSTANCE);
        addJdbcType(MySQLIntUnsignedValue.INSTANCE);
        addJdbcType(MySQLBigIntUnsignedValue.INSTANCE);

        addJdbcTypeName(TINYINT, "TINYINT");
        addJdbcTypeName(SMALLINT, "SMALLINT");
        addJdbcTypeName(INTEGER, "INT({P})");
        addJdbcTypeName(BIGINT, "BIGINT({P})");
        addJdbcTypeName(new JdbcTypeDesc(INTEGER, "MEDIUMINT"), "MEDIUMINT({P})");

        addJdbcTypeName(new MySQLEnumTypeName(), HIGH);
        addJdbcTypeName(new MySQLSetTypeName(), HIGH);
        addJdbcTypeName(new JdbcTypeDesc(DATE, "YEAR"), "YEAR");

        addJdbcTypeName(new JdbcTypeDesc(VARCHAR, "TINYTEXT"), "TINYTEXT");
        addJdbcTypeName(new JdbcTypeDesc(LONGVARCHAR, "TEXT"), "TEXT");
        addJdbcTypeName(new JdbcTypeDesc(LONGVARCHAR, "MEDIUMTEXT"), "MEDIUMTEXT");
        addJdbcTypeName(new JdbcTypeDesc(LONGVARCHAR, "LONGTEXT"), "LONGTEXT");

        addJdbcTypeName(new JdbcTypeDesc(BINARY, "TINYBLOB"), "TINYBLOB");
        addJdbcTypeName(new JdbcTypeDesc(LONGVARBINARY, "BLOB"), "BLOB");
        addJdbcTypeName(new JdbcTypeDesc(LONGVARBINARY, "MEDIUMBLOB"), "MEDIUMBLOB");
        addJdbcTypeName(new JdbcTypeDesc(LONGVARBINARY, "LONGBLOB"), "LONGBLOB");

        addJdbcTypeName(new JdbcTypeDesc(TIMESTAMP), "TIMESTAMP");
        addJdbcTypeName(new JdbcTypeDesc(TIMESTAMP, "DATETIME"), "DATETIME", HIGH);

        addJdbcTypeName(new JdbcTypeDesc(SMALLINT, "SMALLINT UNSIGNED"), "SMALLINT({P}) UNSIGNED");
        addJdbcTypeName(new JdbcTypeDesc(TINYINT, "TINYINT UNSIGNED"), "TINYINT({P}) UNSIGNED");
        addJdbcTypeName(new JdbcTypeDesc(INTEGER, "INT UNSIGNED"), "INT({P}) UNSIGNED");
        addJdbcTypeName(new JdbcTypeDesc(INTEGER, "MEDIUMINT UNSIGNED"), "MEDIUMINT({P}) UNSIGNED");
        addJdbcTypeName(new JdbcTypeDesc(BIGINT, "BIGINT UNSIGNED"), "BIGINT({P}) UNSIGNED");
        // TINYTEXT - A CLOB column with a maximum length of 255 (2**8 - 1)
        // characters.
        // TEXT - A CLOB column with a maximum length of 65,535 (2**16 - 1)
        // characters.
        // MEDIUMTEXT - A CLOB column with a maximum length of 16,777,215 (2**24
        // - 1) characters.
        // LONGTEXT - A CLOB column with a maximum length of 4,294,967,295 or
        // 4GB (2**32 - 1) characters.
        addJdbcTypeName(CLOB, "TEXT");
        addJdbcTypeName(CLOB, newSize(255), "TINYTEXT");
        addJdbcTypeName(CLOB, newSize(65535), "TEXT");
        addJdbcTypeName(CLOB, newSize(16777215), "MEDIUMTEXT");
        addJdbcTypeName(CLOB, newSize(4294967295L), "LONGTEXT");

        addJdbcTypeName(BLOB, "BLOB");
        addJdbcTypeName(BLOB, newSize(255), "TINYBLOB");
        addJdbcTypeName(BLOB, newSize(65535), "BLOB");
        addJdbcTypeName(BLOB, newSize(16777215), "MEDIUMBLOB");
        addJdbcTypeName(BLOB, newSize(4294967295L), "LONGBLOB");

        addJdbcTypeName(REAL, "FLOAT({P},{S})");
        addJdbcTypeName(FLOAT, "FLOAT({P},{S})");
        addJdbcTypeName(DOUBLE, "DOUBLE({P},{S})");
    }

    @Override
    protected void initTranslations() {
        addTranslator(new CurrentTimestampTranslator(NUODB, newArrayList("NOW"), "CURRENT_TIMESTAMP", true));
        addTranslator(new CurrentTimestampTranslator(MYSQL, newArrayList("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()",
                "NOW()", "LOCALTIME", "LOCALTIME()", "LOCALTIMESTAMP", "LOCALTIMESTAMP()"), "CURRENT_TIMESTAMP", true));
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
    public String getInlineColumnTrigger(Session session, ColumnTrigger trigger) {
        StringBuilder result = new StringBuilder("ON");
        result.append(' ');
        result.append(trigger.getTriggerEvent());
        result.append(' ');
        result.append(trigger.getTriggerBody());
        return result.toString();
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
    public boolean supportsIfExistsBeforeDropTable() {
        return true;
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

        StringBuilder value = new StringBuilder(6);
        value.append(rawOffset >= 0 ? '+' : '-');
        value.append(zeros.substring(0, zeros.length() - hoursOffset.length()));
        value.append(hoursOffset);
        value.append(':');
        value.append(zeros.substring(0, zeros.length() - minutesOffset.length()));
        value.append(minutesOffset);
        return value.toString();
    }

    /**
     * Forces driver to stream ResultSet http://goo.gl/kl1Nr
     *
     * @param statement
     *            to stream ResultSet
     * @throws SQLException
     */
    @Override
    public void setFetchMode(Statement statement, FetchMode fetchMode) throws SQLException {
        statement.setFetchSize(fetchMode.isStream() ? MIN_VALUE : fetchMode.getFetchSize());
    }

    /**
     * Will handle only MySQL triggers
     *
     * @param session
     *            source session
     * @param trigger
     *            column trigger to inline
     * @return result script
     */
    @Override
    public boolean supportInlineColumnTrigger(Session session, ColumnTrigger trigger) {
        return trigger != null && MYSQL.isAssignable(session.getDatabaseInfo());
    }

    @Override
    public boolean supportsIfExistsBeforeDropTrigger() {
        return true;
    }

    @Override
    public boolean supportsLimit() {
        return true;
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
    public LimitHandler createLimitHandler(String query, QueryLimit queryLimit) {
        return new MySQLLimitHandler(this, query, queryLimit);
    }

    @Override
    public boolean supportsRowCount(Table table, Column column, String filter, RowCountType rowCountType) {
        return rowCountType == APPROX || rowCountType == EXACT;
    }

    @Override
    public RowCountHandler createRowCountHandler(Table table, Column column, String filter, RowCountType rowCountType) {
        return new MySQLTableRowCountHandler(this, table, column, filter, rowCountType);
    }
}
