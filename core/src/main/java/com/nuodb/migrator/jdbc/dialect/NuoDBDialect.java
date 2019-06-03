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
import com.nuodb.migrator.jdbc.metadata.ReferenceAction;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.Trigger;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;

import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.dialect.OracleDialect.*;
import static com.nuodb.migrator.jdbc.dialect.PostgreSQLDialect.BIT_DESC;
import static com.nuodb.migrator.jdbc.dialect.PostgreSQLDialect.BIT_VARYING_DESC;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.*;
import static com.nuodb.migrator.jdbc.type.JdbcTypeNames.createEnumTypeNameTemplate;
import static com.nuodb.migrator.jdbc.type.JdbcTypeNames.createTypeNameTemplate;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.*;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.Types.*;
import static java.util.regex.Pattern.compile;
import static org.apache.commons.lang3.StringUtils.isAllUpperCase;

/**
 * @author Sergey Bushik
 */
public class NuoDBDialect extends SimpleDialect {

    private static final Pattern ALLOWED_IDENTIFIER_PATTERN = compile("^[a-zA-Z_]+\\w*$");

    private static final int WRITE_COMMITTED = 5;
    private static final int CONSISTENT_READ = 7;

    public NuoDBDialect() {
        this(NUODB_BASE);
    }

    public NuoDBDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
    }

    @Override
    protected void initJdbcTypes() {
        addJdbcType(NuoDBSmallIntValue.INSTANCE);
        addJdbcType(NuoDBIntegerValue.INSTANCE);
        addJdbcType(NuoDBBigIntValue.INSTANCE);
        addJdbcType(NuoDBTimeValue.INSTANCE);
    }

    @Override
    protected void initJdbcTypeNames() {
        addJdbcTypeName(BIT, newSize(0), "BOOLEAN");
        addJdbcTypeName(BIT, newSize(1), "BOOLEAN");
        addJdbcTypeName(TINYINT, "SMALLINT");
        addJdbcTypeName(SMALLINT, "SMALLINT");
        addJdbcTypeName(INTEGER, "INTEGER");
        addJdbcTypeName(BIGINT, "BIGINT");

        // fix for integer & bigint
        addJdbcTypeName(new JdbcTypeDesc(INTEGER, "INTEGER"), newScale(0), "INTEGER");
        addJdbcTypeName(new JdbcTypeDesc(INTEGER, "INTEGER"), "NUMERIC({P},{S})");
        addJdbcTypeName(new JdbcTypeDesc(BIGINT, "BIGINT"), newScale(0), "BIGINT");
        addJdbcTypeName(new JdbcTypeDesc(BIGINT, "BIGINT"), "NUMERIC({P},{S})");

        addJdbcTypeName(REAL, "REAL");
        addJdbcTypeName(FLOAT, "FLOAT");
        addJdbcTypeName(DOUBLE, "DOUBLE");
        addJdbcTypeName(NUMERIC, "NUMERIC({P},{S})");
        addJdbcTypeName(DECIMAL, "DECIMAL({P},{S})");
        addJdbcTypeName(new JdbcTypeDesc(DECIMAL, "NUMBER"), newScale(0), "NUMBER");

        addJdbcTypeName(CHAR, newSize(0), "CHAR");
        addJdbcTypeName(CHAR, "CHAR({N})");

        addJdbcTypeName(VARCHAR, "VARCHAR({N})");
        addJdbcTypeName(LONGVARCHAR, "VARCHAR({N})");

        addJdbcTypeName(DATE, "DATE");
        addJdbcTypeName(TIME, newScale(0), "TIME");
        addJdbcTypeName(TIME, "TIME({S})");
        addJdbcTypeName(TIMESTAMP, newScale(0), "TIMESTAMP");
        addJdbcTypeName(TIMESTAMP, "TIMESTAMP({S})");

        addJdbcTypeName(BINARY, "BINARY({N})");
        addJdbcTypeName(VARBINARY, "VARBINARY({N})");
        addJdbcTypeName(LONGVARBINARY, "VARBINARY({N})");

        // For NuoDB 'binary' and 'binary varying' datatypes
        addJdbcTypeName(new JdbcTypeDesc(BLOB, "BINARY"), "BINARY({N})");
        addJdbcTypeName(new JdbcTypeDesc(BLOB, "BINARY VARYING"), "VARBINARY({N})");

        addJdbcTypeName(NULL, "NULL");
        addJdbcTypeName(BLOB, "BLOB");
        addJdbcTypeName(CLOB, "CLOB");
        addJdbcTypeName(BOOLEAN, "BOOLEAN");

        addJdbcTypeName(NCHAR, "NCHAR({N})");
        addJdbcTypeName(NCHAR, newSize(0), "NCHAR");
        addJdbcTypeName(NVARCHAR, "NVARCHAR({N})");
        addJdbcTypeName(NCLOB, "NCLOB");
        addJdbcTypeName(ROWID, "STRING");
        addJdbcTypeName(new JdbcTypeDesc(VARCHAR, "STRING"), "STRING");
        addJdbcTypeName(new JdbcTypeDesc(SQLXML), "STRING");

        addJdbcTypeName(ORACLE, new JdbcTypeDesc(LONGVARCHAR, "LONG"), "CLOB");
        addJdbcTypeName(ORACLE, new JdbcTypeDesc(LONGVARBINARY, "LONG RAW"), "BLOB");
        addJdbcTypeName(ORACLE, DECIMAL, "NUMBER", newOptions(0, 0, 0));
        addJdbcTypeName(ORACLE, XMLTYPE_DESC, "STRING");
        addJdbcTypeName(ORACLE, ANYDATA_DESC, "STRING");
        addJdbcTypeName(ORACLE, ANYDATASET_DESC, "STRING");
        addJdbcTypeName(ORACLE, ANYTYPE_DESC, "STRING");
        addJdbcTypeName(ORACLE, BFILE_DESC, "BLOB");
        addJdbcTypeName(ORACLE, USER_DEFINED_VARRAY_DESC, "BLOB");
        addJdbcTypeName(ORACLE, USER_DEFINED_STRUCT_DESC, "BLOB");
        addJdbcTypeName(ORACLE, USER_DEFINED_REF_DESC, "BLOB");

        addJdbcTypeName(POSTGRE_SQL, BIT_DESC, "STRING");
        addJdbcTypeName(POSTGRE_SQL, BIT_VARYING_DESC, "STRING");

        // TINYTEXT has a maximum length of 255
        addJdbcTypeName(MYSQL, new JdbcTypeDesc(VARCHAR, "TINYTEXT"), "VARCHAR({N})");
        // TEXT has a maximum length of 65,535
        addJdbcTypeName(MYSQL, new JdbcTypeDesc(LONGVARCHAR, "TEXT"), "CLOB");
        // MEDIUMTEXT has a maximum length of 16,777,215
        addJdbcTypeName(MYSQL, new JdbcTypeDesc(LONGVARCHAR, "MEDIUMTEXT"), "CLOB");
        // LONGTEXT has a maximum length of 4,294,967,295
        addJdbcTypeName(MYSQL, new JdbcTypeDesc(LONGVARCHAR, "LONGTEXT"), "CLOB");

        addJdbcTypeName(MYSQL, new JdbcTypeDesc(BINARY, "TINYBLOB"), "VARBINARY({N})");
        addJdbcTypeName(MYSQL, new JdbcTypeDesc(LONGVARBINARY, "BLOB"), "BLOB");
        addJdbcTypeName(MYSQL, new JdbcTypeDesc(LONGVARBINARY, "MEDIUMBLOB"), "BLOB");
        addJdbcTypeName(MYSQL, new JdbcTypeDesc(LONGVARBINARY, "LONGBLOB"), "BLOB");

        addJdbcTypeName(MYSQL, new JdbcTypeDesc(SMALLINT, "SMALLINT UNSIGNED"), "INTEGER");
        addJdbcTypeName(MYSQL, new JdbcTypeDesc(INTEGER, "INT UNSIGNED"), "BIGINT");
        addJdbcTypeName(MYSQL,
                createTypeNameTemplate(new JdbcTypeDesc(BIGINT, "BIGINT UNSIGNED"), "NUMERIC({N})", newSize(1)));

        addJdbcTypeName(DB2, new JdbcTypeDesc(LONGVARBINARY, "LONG VARCHAR FOR BIT DATA"), "VARCHAR({N})");
        addJdbcTypeName(DB2, new JdbcTypeDesc(OTHER, "DECFLOAT"), "DECIMAL({P},{S})");
        addJdbcTypeName(DB2, new JdbcTypeDesc(OTHER, "XML"), "STRING");

        addJdbcTypeName(MSSQL_SERVER, new JdbcTypeDesc(CLOB, "XML"), "STRING");

        addJdbcTypeName(createEnumTypeNameTemplate("CHAR({N})"));
    }

    @Override
    protected void initTranslations() {
        addTranslator(new CurrentTimestampTranslator(NUODB_BASE,
                newArrayList("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()", "NOW()"), "CURRENT_TIMESTAMP", true));
        addTranslator(new CurrentTimestampTranslator(MYSQL, newArrayList("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()",
                "NOW()", "LOCALTIME", "LOCALTIME()", "LOCALTIMESTAMP", "LOCALTIMESTAMP()"), "NOW"));
        addTranslator(new MySQLBitLiteralTranslator());
        addTranslator(new MySQLHexLiteralTranslator());
        addTranslator(new MySQLZeroDateTimeTranslator());
        addTranslator(new MySQLImplicitDefaultsTranslator());
        addTranslator(new MySQLOnUpdateTriggerTranslator());

        addTranslator(new CurrentTimestampTranslator(MSSQL_SERVER,
                newArrayList("GETDATE()", "CURRENT_TIMESTAMP", "NOW()"), "NOW"));
        addTranslationRegex(MSSQL_SERVER, "N'(.*)'", "$1");

        addTranslator(new CurrentTimestampTranslator(POSTGRE_SQL, newArrayList("CURRENT_TIMESTAMP", "NOW()"), "NOW"));
        addTranslationRegex(POSTGRE_SQL, "'(.*)'::.*", "$1");

        addTranslator(new CurrentTimestampTranslator(ORACLE, newArrayList("CURRENT_DATE", "SYSDATE"), "NOW"));

        addTranslator(new CurrentTimestampTranslator(DB2,
                newArrayList("CURRENT DATE", "CURRENT TIME", "CURRENT TIMESTAMP"), "NOW"));
    }

    @Override
    public boolean isQuotingIdentifier(String identifier, Identifiable identifiable) {
        return super.isQuotingIdentifier(identifier, identifiable) || !isAllUpperCase(identifier);
    }

    @Override
    public boolean isAllowedIdentifier(String identifier, Identifiable identifiable) {
        return ALLOWED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    @Override
    public SQLKeywords getSQLKeywords() {
        return SQLKeywords.NUODB_KEYWORDS;
    }

    @Override
    public boolean isSQLKeyword(String identifier, Identifiable identifiable) {
        return !(identifiable != null && identifiable instanceof Table) && getSQLKeywords().contains(identifier);
    }

    @Override
    public boolean supportsTransactionIsolation(int level) {
        boolean supports = false;
        switch (level) {
        case WRITE_COMMITTED:
        case CONSISTENT_READ:
        case TRANSACTION_READ_COMMITTED:
        case TRANSACTION_SERIALIZABLE:
            supports = true;
            break;
        }
        return supports;
    }

    @Override
    public boolean supportsSequence() {
        return true;
    }

    @Override
    public String getSequenceStartWith(Number startWith) {
        return startWith != null ? "START WITH " + startWith : null;
    }

    @Override
    public String getSequenceIncrementBy(Number incrementBy) {
        return null;
    }

    @Override
    public String getSequenceMinValue(Number minValue) {
        return null;
    }

    @Override
    public String getSequenceMaxValue(Number maxValue) {
        return null;
    }

    @Override
    public String getSequenceCache(Number cache) {
        return null;
    }

    @Override
    public String getSequenceCycle(boolean cycle) {
        return null;
    }

    @Override
    public String getSequenceOrder(boolean order) {
        return null;
    }

    @Override
    public boolean supportsStatementWithTimezone() {
        return false;
    }

    // @Override
    // public boolean supportsSessionTimeZone() {
    // return true;
    // }
    //
    // @Override
    // public void setSessionTimeZone(Connection connection, TimeZone timeZone)
    // throws SQLException {
    // timeZone = timeZone == null ? getDefault() : timeZone;
    // RemConnection remConnection = (RemConnection) (connection instanceof
    // ConnectionProxy ?
    // ((ConnectionProxy)connection).getConnection() : connection);
    // SQLContext sqlContext = remConnection.getSqlContext();
    // sqlContext.setTimeZone(timeZone);
    // sqlContext.setTimeZoneId(timeZone.getID());
    // }

    @Override
    public boolean supportsDropConstraints() {
        return false;
    }

    @Override
    public boolean supportsDropPrimaryKey() {
        return false;
    }

    @Override
    public boolean supportsIfExistsAfterDropTrigger() {
        return true;
    }

    @Override
    public boolean supportsDropIndexOnTable() {
        return false;
    }

    @Override
    public boolean supportsDropIndexIfExists() {
        return true;
    }

    @Override
    public boolean supportsIndexInCreateTable() {
        return false;
    }

    @Override
    public boolean supportsIfExistsBeforeDropTable() {
        return true;
    }

    @Override
    public boolean supportsIfExistsAfterDropTable() {
        return true;
    }

    @Override
    public boolean supportsDropSequenceIfExists() {
        return true;
    }

    @Override
    public String getCascadeConstraints() {
        return "CASCADE";
    }

    @Override
    public String getDeleteAction(ReferenceAction deleteAction) {
        switch (deleteAction) {
        case CASCADE:
            return "CASCADE";
        default:
            return null;
        }
    }

    /**
     * Returns true if DDL scripts for a primary key, indexes and foreign keys
     * should be generated as a separate statements to speed up performance of
     * migration.
     *
     * @param table
     * @return false for NuoDB
     */
    @Override
    public boolean addScriptsInCreateTable(Table table) {
        return false;
    }

    @Override
    public String getDropForeignKey() {
        return "DROP FOREIGN KEY";
    }

    @Override
    public String getIdentityColumn(String sequence) {
        StringBuilder buffer = new StringBuilder(32 + (sequence != null ? sequence.length() + 2 : 0));
        buffer.append("GENERATED BY DEFAULT AS IDENTITY");
        if (sequence != null) {
            buffer.append('(');
            buffer.append(sequence);
            buffer.append(')');
        }
        return buffer.toString();
    }

    @Override
    public String getTriggerActive(boolean active) {
        return active ? "ACTIVE" : "INACTIVE";
    }

    @Override
    public String getTriggerOn(Table table) {
        return "FOR";
    }

    @Override
    public String getTriggerBegin(Trigger trigger) {
        return "AS";
    }

    @Override
    public String getTriggerEnd(Trigger trigger) {
        return "END_TRIGGER";
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
        return new NuoDBLimitHandler(this, query, queryLimit);
    }

    @Override
    public boolean supportsRowCount(Table table, Column column, String filter, RowCountType rowCountType) {
        // row count disabled temporary for performance reasons
        return false;
    }
}