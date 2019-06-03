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
import com.nuodb.migrator.jdbc.query.StatementTemplate;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.type.JdbcEnumType;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.url.JdbcUrl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.dialect.MySQLZeroDateTimeTranslator.*;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.url.MySQLJdbcUrl.*;
import static java.lang.String.valueOf;
import static java.sql.Types.*;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.split;

/**
 * Allow for MySQL implicit defaults to move over to the NuoDB schema explicitly
 * <a hre="http://dev.mysql.com/doc/refman/5.5/en/data-type-defaults.html">MySQL
 * Data Type Defaults</a>
 *
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class MySQLImplicitDefaultsTranslator extends ImplicitDefaultsTranslatorBase {
    /**
     * Does not check if strict mode is set, as it's always set by MySQL JDBC
     * driver for the
     */
    public static final boolean CHECK_SQL_MODE = false;
    /**
     * Session key for the sql mode collection
     */
    public static final String SQL_MODE = "SQL_MODE";
    public static final String STRICT_ALL_TABLES = "STRICT_ALL_TABLES";
    public static final String STRICT_TRANS_TABLES = "STRICT_TRANS_TABLES";
    /**
     * Default query to retrieve global sql mode
     */
    public static final String SQL_MODE_QUERY = "SELECT @@GLOBAL.SQL_MODE";

    private boolean checkSqlMode = CHECK_SQL_MODE;
    private String sqlModeQuery = SQL_MODE_QUERY;

    public MySQLImplicitDefaultsTranslator() {
        super(MYSQL);
    }

    /**
     * Validates that the script matches requirements for implicit defaults,
     * such as script is null, column is not nullable and column is not an
     * identity column
     *
     * @param script
     *            to be checked for requirements
     * @return true is all requirements are honoured
     */
    protected boolean hasExplicitDefaults(ColumnScript script) {
        Column column = script.getColumn();
        return script.getScript() == null && !column.isNullable() && !column.isAutoIncrement();
    }

    protected boolean isUseExplicitDefaults(TranslationContext context) {
        return super.isUseExplicitDefaults(context) && (!isCheckSqlMode() || checkSqlMode(context));
    }

    /**
     * Checks if any of the strict modes [STRICT_ALL_TABLE, STRICT_TRANS_TABLES]
     * is on for this session
     *
     * @param context
     *            translation context
     * @return true if any of the strict modes is on
     */
    protected boolean checkSqlMode(TranslationContext context) {
        Collection<String> sqlMode = getSqlMode(context.getSession(), false);
        return contains(sqlMode, STRICT_ALL_TABLES) || contains(sqlMode, STRICT_TRANS_TABLES);
    }

    protected Collection<String> getSqlMode(Session session, boolean read) {
        Collection<String> sqlModes = (Collection<String>) session.get(SQL_MODE);
        if (sqlModes == null || read) {
            try {
                session.put(SQL_MODE, sqlModes = getSqlMode(session.getConnection()));
            } catch (SQLException exception) {
                throw new TranslatorException("Can't read SQL mode", exception);
            }
        }
        return sqlModes;
    }

    protected Collection<String> getSqlMode(Connection connection) throws SQLException {
        final Collection<String> sqlMode = newArrayList();
        StatementTemplate template = new StatementTemplate(connection);
        template.executeStatement(new StatementFactory<Statement>() {
            @Override
            public Statement createStatement(Connection connection) throws SQLException {
                return connection.createStatement();
            }
        }, new StatementCallback<Statement>() {
            @Override
            public void executeStatement(Statement statement) throws SQLException {
                ResultSet resultSet = statement.executeQuery(getSqlModeQuery());
                while (resultSet.next()) {
                    sqlMode.addAll(asList(split(resultSet.getString(1), ',')));
                }
            }
        });
        return sqlMode;
    }

    /**
     * If strict mode is not enabled, MySQL sets the column to the implicit
     * default value for the column data type:
     * <ul>
     * <li>For numeric types, the default is 0</li>
     * <li>Integer or floating-point types declared with the AUTO_INCREMENT
     * attribute, the default is the next value in the sequence. This is handled
     * by sequences</li>
     * <li>For string types other than ENUM, the default value is the empty
     * string</li>
     * <li>For ENUM, the default is the first enumeration value</li>
     * <li>For date and time types other than TIMESTAMP, the default is the
     * appropriate "zero" value for the type, we skip this</li>
     * <li>For the first TIMESTAMP column in a table, the default value is the
     * current date and time. This is converted to an explicit thing by MySQL,
     * so we are OK</li>
     * </ul>
     *
     * @param script
     *            default source script to translate
     * @param context
     *            translation context
     * @return string translated according to implicit rules
     */
    @Override
    public Script translate(ColumnScript script, TranslationContext context) {
        String result;
        Column column = script.getColumn();
        JdbcUrl jdbcUrl = getJdbcUrl(context);
        String behavior = (String) jdbcUrl.getParameters().get(ZERO_DATE_TIME_BEHAVIOR);
        if (behavior == null) {
            behavior = DEFAULT_BEHAVIOR;
        }
        switch (column.getTypeCode()) {
        case BIT:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case REAL:
        case DOUBLE:
        case NUMERIC:
        case DECIMAL:
            result = valueOf(0);
            break;
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
        case BLOB:
        case CLOB:
            JdbcType jdbcType = column.getJdbcType();
            if (jdbcType instanceof JdbcEnumType) {
                result = get(((JdbcEnumType) jdbcType).getValues(), 0);
            } else {
                result = EMPTY;
            }
            break;
        case DATE:
            if (!behavior.equals(CONVERT_TO_NULL)) {
                result = context.translate(new ColumnScript(column, ZERO_DATE)).getScript();
            } else {
                result = script.getScript();
            }
            break;
        case TIME:
            if (!behavior.equals(CONVERT_TO_NULL)) {
                result = context.translate(new ColumnScript(column, ZERO_TIME)).getScript();
            } else {
                result = script.getScript();
            }
            break;
        case TIMESTAMP:
            if (!behavior.equals(CONVERT_TO_NULL)) {
                result = context.translate(new ColumnScript(column, ZERO_TIMESTAMP)).getScript();
            } else {
                result = script.getScript();
            }
            break;
        default:
            result = script.getScript();
        }
        // case ENUM, and SET
        return result != null ? new SimpleScript(result) : null;
    }

    public boolean isCheckSqlMode() {
        return checkSqlMode;
    }

    public void setCheckSqlMode(boolean checkSqlMode) {
        this.checkSqlMode = checkSqlMode;
    }

    public String getSqlModeQuery() {
        return sqlModeQuery;
    }

    public void setSqlModeQuery(String sqlModeQuery) {
        this.sqlModeQuery = sqlModeQuery;
    }

    @Override
    protected boolean supportsScript(Script script, TranslationContext context) {
        return false;
    }
}
