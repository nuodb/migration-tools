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

import com.nuodb.migrator.jdbc.JdbcUtils;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.type.*;
import com.nuodb.migrator.jdbc.url.JdbcUrl;
import com.nuodb.migrator.match.Regex;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TimeZone;

import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.APPROX;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static com.nuodb.migrator.match.AntRegexCompiler.INSTANCE;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.Types.*;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class OracleDialect extends SimpleDialect {

    public static final JdbcTypeDesc BFILE_DESC = new JdbcTypeDesc(-13, "BFILE");
    public static final JdbcTypeDesc ANYDATA_DESC = new JdbcTypeDesc(OTHER, "ANYDATA");
    public static final JdbcTypeDesc ANYDATASET_DESC = new JdbcTypeDesc(OTHER, "ANYDATASET");
    public static final JdbcTypeDesc ANYTYPE_DESC = new JdbcTypeDesc(OTHER, "ANYTYPE");
    public static final JdbcTypeDesc XMLTYPE_DESC = new JdbcTypeDesc(SQLXML, "XMLTYPE");
    public static final JdbcTypeDesc BINARY_FLOAT_DESC = new JdbcTypeDesc(100, "BINARY_FLOAT");
    public static final JdbcTypeDesc BINARY_DOUBLE_DESC = new JdbcTypeDesc(101, "BINARY_DOUBLE");
    public static final JdbcTypeDesc TIMESTAMP_WITH_TIME_ZONE_DESC = new JdbcTypeDesc(-101);
    public static final JdbcTypeDesc TIMESTAMP_WITH_LOCAL_TIME_ZONE_DESC = new JdbcTypeDesc(-102);
    public static final JdbcTypeDesc INTERVAL_YEAR_TO_MONTH_DESC = new JdbcTypeDesc(-103);
    public static final JdbcTypeDesc INTERVAL_DAY_TO_SECOND_DESC = new JdbcTypeDesc(-104);
    public static final JdbcTypeDesc USER_DEFINED_VARRAY_DESC = new JdbcTypeDesc(OTHER, "ARRAY");
    public static final JdbcTypeDesc USER_DEFINED_STRUCT_DESC = new JdbcTypeDesc(OTHER, "STRUCT");
    public static final JdbcTypeDesc USER_DEFINED_REF_DESC = new JdbcTypeDesc(OTHER, "REF");

    private static final Regex TIMESTAMP_REGEX = INSTANCE.compile("TIMESTAMP(*)");
    private static final Regex TIMESTAMP_WITH_TIME_ZONE_REGEX = INSTANCE.compile("TIMESTAMP(*) WITH TIME ZONE");
    private static final Regex TIMESTAMP_WITH_TIME_LOCAL_ZONE_REGEX = INSTANCE
            .compile("TIMESTAMP(*) WITH LOCAL TIME ZONE");
    public static final Regex INTERVAL_YEAR_TO_MATCH_REGEX = INSTANCE.compile("INTERVAL YEAR(*) TO MONTH");
    public static final Regex INTERVAL_DAY_TO_SECOND_REGEX = INSTANCE.compile("INTERVAL DAY(*) TO SECOND(*)");

    private static int CL_MAX_OPEN_CURSORS = 0;
    private static final int DEFAULT_MAX_OPEN_CURSORS = 300;
    private static final String MAX_OPEN_CURSORS = "maxopencursors";

    public OracleDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
    }

    @Override
    protected void initJdbcTypes() {
        super.initJdbcTypes();
        addJdbcType(OracleXmlTypeValue.INSTANCE);
        addJdbcType(OracleBFileValue.INSTANCE);
        addJdbcTypeAdapter(new OracleBlobTypeAdapter());
        addJdbcTypeAdapter(new OracleClobTypeAdapter());
    }

    @Override
    protected void initJdbcTypeNames() {
        super.initJdbcTypeNames();
        addJdbcTypeAlias(OTHER, "XMLTYPE", SQLXML);
        addJdbcTypeAlias(OTHER, "ROWID", ROWID);
        addJdbcTypeAlias(OTHER, "UROWID", ROWID);
        addJdbcTypeAlias(OTHER, "NCHAR", NCHAR);
        addJdbcTypeAlias(OTHER, "NVARCHAR2", NVARCHAR);
        addJdbcTypeAlias(OTHER, "NCLOB", NCLOB);
        addJdbcTypeAlias(BINARY_FLOAT_DESC, FLOAT);
        addJdbcTypeAlias(BINARY_DOUBLE_DESC, DOUBLE);
        addJdbcTypeAlias(TIMESTAMP_WITH_TIME_ZONE_DESC, TIMESTAMP);
        addJdbcTypeAlias(TIMESTAMP_WITH_LOCAL_TIME_ZONE_DESC, TIMESTAMP);
        addJdbcTypeAlias(INTERVAL_DAY_TO_SECOND_DESC, VARCHAR);
        addJdbcTypeAlias(INTERVAL_YEAR_TO_MONTH_DESC, VARCHAR);
    }

    @Override
    public Integer getMaxOpenCursors(Connection connection) throws SQLException {
        ResultSet result = null;
        String userName = null;
        Statement statement = connection.createStatement();
        JdbcUrl jdbcUrl = JdbcUtils.getJdbcUrl(connection);
        Map<String, Object> properties = jdbcUrl.getParameters();
        if (!isEmpty(properties) && properties.containsKey(MAX_OPEN_CURSORS)) {
            if (Integer.valueOf(String.valueOf(properties.get(MAX_OPEN_CURSORS))) > 0) {
                CL_MAX_OPEN_CURSORS = Integer.valueOf(String.valueOf(properties.get(MAX_OPEN_CURSORS)));
            }
        }
        try {
            userName = connection.getMetaData().getUserName();
            result = statement.executeQuery("SELECT VALUE FROM V$PARAMETER WHERE NAME = 'open_cursors'");
        } catch (Exception e) {
            int MAX = CL_MAX_OPEN_CURSORS == 0 ? DEFAULT_MAX_OPEN_CURSORS : CL_MAX_OPEN_CURSORS;
            if (logger.isWarnEnabled()) {
                logger.warn((format(
                        " Oracle user %s don't have permission to access V$PARAMETER view, please contact Database Administrator ",
                        userName)));
                logger.warn((format(
                        " Value for max_open_cursors set to %d. This might result in ORA-01000: maximum open cursors exceeded exception",
                        MAX)));
            }
        }
        if (!(result == null) && result.next()) {
            return maxOpenCursorsValue(result.getInt(1), CL_MAX_OPEN_CURSORS, DEFAULT_MAX_OPEN_CURSORS);
        } else {
            return maxOpenCursorsValue(0, CL_MAX_OPEN_CURSORS, DEFAULT_MAX_OPEN_CURSORS);
        }
    }

    public Integer maxOpenCursorsValue(int sMax, int clMax, int defMax) {
        if (clMax != 0) {
            if (sMax != 0 && sMax < clMax) {
                if (logger.isWarnEnabled()) {
                    logger.warn((format(
                            "Oracle max_open_cursors value is %d and command line passing value is %d. This might result in ORA-01000: maximum open cursors exceeded exception ",
                            sMax, clMax)));
                }
                return clMax;
            } else {
                return clMax;
            }
        } else {
            if (sMax != 0) {
                return sMax;
            } else {
                return defMax;
            }
        }
    }

    @Override
    public JdbcTypeDesc getJdbcTypeAlias(int typeCode, String typeName) {
        JdbcTypeDesc jdbcTypeAlias = null;
        if (typeCode == OTHER) {
            if (TIMESTAMP_REGEX.test(typeName)) {
                jdbcTypeAlias = new JdbcTypeDesc(TIMESTAMP);
            } else if (TIMESTAMP_WITH_TIME_ZONE_REGEX.test(typeName)) {
                jdbcTypeAlias = new JdbcTypeDesc(TIMESTAMP);
            } else if (TIMESTAMP_WITH_TIME_LOCAL_ZONE_REGEX.test(typeName)) {
                jdbcTypeAlias = new JdbcTypeDesc(TIMESTAMP);
            } else if (INTERVAL_YEAR_TO_MATCH_REGEX.test(typeName)) {
                jdbcTypeAlias = new JdbcTypeDesc(VARCHAR);
            } else if (INTERVAL_DAY_TO_SECOND_REGEX.test(typeName)) {
                jdbcTypeAlias = new JdbcTypeDesc(VARCHAR);
            }
        }
        if (jdbcTypeAlias == null) {
            jdbcTypeAlias = super.getJdbcTypeAlias(typeCode, typeName);
        }
        return jdbcTypeAlias;
    }

    @Override
    public boolean supportsTransactionIsolation(int level) {
        boolean supports = false;
        switch (level) {
        case TRANSACTION_READ_COMMITTED:
        case TRANSACTION_SERIALIZABLE:
            supports = true;
            break;
        }
        return supports;
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
            statement.execute("ALTER SESSION SET TIME_ZONE = " + timeZoneAsValue);
        } finally {
            closeQuietly(statement);
        }
    }

    protected String timeZoneAsValue(TimeZone timeZone) {
        int rawOffset = timeZone.getRawOffset();
        int dstSavings = timeZone.getDSTSavings();
        int absOffset = Math.abs(rawOffset + dstSavings);
        String hoursOffset = Integer.toString(absOffset / 3600000);
        String minutesOffset = Integer.toString((absOffset % 3600000) / 60000);

        String zeros = "00";
        StringBuilder value = new StringBuilder(34);
        value.append("'");
        value.append(rawOffset >= 0 ? '+' : '-');
        value.append(zeros.substring(0, zeros.length() - hoursOffset.length()));
        value.append(hoursOffset);
        value.append(':');
        value.append(zeros.substring(0, zeros.length() - minutesOffset.length()));
        value.append("'");
        return value.toString();
    }

    @Override
    public boolean supportsDropConstraints() {
        return false;
    }

    @Override
    public boolean supportsNegativeScale() {
        return true;
    }

    @Override
    public String getCascadeConstraints() {
        return "CASCADE CONSTRAINTS";
    }

    @Override
    public String getSequenceMinValue(Number minValue) {
        return minValue != null ? "MINVALUE " + minValue : "NOMINVALUE";
    }

    @Override
    public String getSequenceMaxValue(Number maxValue) {
        return maxValue != null ? "MAXVALUE " + maxValue : "NOMAXVALUE";
    }

    @Override
    public String getSequenceCycle(boolean cycle) {
        return cycle ? "CYCLE" : "NOCYCLE";
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
    public boolean supportsSchemas() {
        return true;
    }

    @Override
    public LimitHandler createLimitHandler(String query, QueryLimit queryLimit) {
        return new OracleLimitHandler(this, query, queryLimit);
    }

    @Override
    public boolean supportsRowCount(Table table, Column column, String filter, RowCountType rowCountType) {
        return (rowCountType == APPROX && column == null && filter == null) || (rowCountType == EXACT);
    }

    @Override
    public RowCountHandler createRowCountHandler(Table table, Column column, String filter, RowCountType rowCountType) {
        return new OracleTableRowCountHandler(this, table, column, filter, rowCountType);
    }
}
