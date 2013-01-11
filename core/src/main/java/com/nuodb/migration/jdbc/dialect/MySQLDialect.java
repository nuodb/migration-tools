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
package com.nuodb.migration.jdbc.dialect;

import com.nuodb.migration.jdbc.resolve.DatabaseInfo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.TimeZone;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static java.lang.String.valueOf;

/**
 * @author Sergey Bushik
 */
public class MySQLDialect extends SimpleDialect {

    public MySQLDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);

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
