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
import com.nuodb.migrator.jdbc.metadata.Identifiable;
import com.nuodb.migrator.jdbc.metadata.ReferenceAction;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.jdbc.type.JdbcTypeNameChangeSpecifier;

import java.sql.Types;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.resolve.DatabaseInfoUtils.*;
import static com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers.newScale;
import static com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers.newSize;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
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
        this(NUODB);
    }

    public NuoDBDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
    }

    @Override
    protected void initJdbcTypes() {
        addJdbcType(NuoDBSmallIntType.INSTANCE);
        addJdbcType(NuoDBIntegerType.INSTANCE);
        addJdbcType(NuoDBBigIntType.INSTANCE);
        addJdbcType(NuoDBTimeType.INSTANCE);
    }

    @Override
    protected void initJdbcTypeNames() {
        addJdbcTypeName(Types.BIT, newSize(0), "BOOLEAN");
        addJdbcTypeName(Types.BIT, newSize(1), "BOOLEAN");
        addJdbcTypeName(Types.TINYINT, "SMALLINT");
        addJdbcTypeName(Types.SMALLINT, "SMALLINT");

        addJdbcTypeName(Types.INTEGER, "INTEGER");

        addJdbcTypeName(Types.BIGINT, "BIGINT");
        addJdbcTypeName(Types.NUMERIC, "NUMERIC({P},{S})");
        addJdbcTypeName(Types.DECIMAL, "DECIMAL({P},{S})");

        addJdbcTypeName(Types.REAL, "REAL");
        addJdbcTypeName(Types.FLOAT, "FLOAT");
        addJdbcTypeName(Types.DOUBLE, "DOUBLE");
        addJdbcTypeName(Types.DECIMAL, "DECIMAL({P},{S})");

        addJdbcTypeName(Types.CHAR, "CHAR({N})");

        addJdbcTypeName(Types.VARCHAR, "VARCHAR({N})");
        addJdbcTypeName(Types.LONGVARCHAR, "VARCHAR({N})");

        addJdbcTypeName(Types.DATE, "DATE");
        addJdbcTypeName(Types.TIME, newScale(0), "TIME");
        addJdbcTypeName(Types.TIME, "TIME({S})");
        addJdbcTypeName(Types.TIMESTAMP, newScale(0), "TIMESTAMP");
        addJdbcTypeName(Types.TIMESTAMP, "TIMESTAMP({S})");

        addJdbcTypeName(Types.BINARY, "BINARY({N})");
        addJdbcTypeName(Types.VARBINARY, "VARBINARY({N})");
        addJdbcTypeName(Types.LONGVARBINARY, "VARBINARY({N})");

        addJdbcTypeName(Types.NULL, "NULL");
        addJdbcTypeName(Types.BLOB, "BLOB");
        addJdbcTypeName(Types.CLOB, "CLOB");
        addJdbcTypeName(Types.BOOLEAN, "BOOLEAN");

        addJdbcTypeName(Types.NCHAR, "NCHAR({N})");
        addJdbcTypeName(Types.NVARCHAR, "NVARCHAR({N})");
        addJdbcTypeName(Types.NCLOB, "NCLOB");
        addJdbcTypeName(new JdbcTypeDesc(Types.VARCHAR, "STRING"), "STRING");

        addJdbcTypeName(NUODB, new JdbcTypeDesc(Types.SMALLINT, "SMALLINT UNSIGNED"), "INTEGER");
        addJdbcTypeName(NUODB, new JdbcTypeDesc(Types.INTEGER, "INT UNSIGNED"), "BIGINT");
        addJdbcTypeName(NUODB, new JdbcTypeDesc(Types.BIGINT, "BIGINT UNSIGNED"),
                new JdbcTypeNameChangeSpecifier("NUMERIC({N})", 1));

        addJdbcTypeName(DB2, new JdbcTypeDesc(Types.LONGVARBINARY, "LONG VARCHAR FOR BIT DATA"), "VARCHAR({N})");
        addJdbcTypeName(DB2, new JdbcTypeDesc(Types.OTHER, "DECFLOAT"), "DECIMAL({P},{S})");
        addJdbcTypeName(DB2, new JdbcTypeDesc(Types.OTHER, "XML"), "CLOB");
    }

    @Override
    protected void initTranslations() {
        addTranslation(MYSQL, newArrayList("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()", "NOW()",
                "LOCALTIME", "LOCALTIME()", "LOCALTIMESTAMP", "LOCALTIMESTAMP()"), "NOW");
        addTranslator(new MySQLBitLiteralTranslator());

        addTranslation(MSSQL_SERVER, newArrayList("GETDATE()", "CURRENT_TIMESTAMP", "NOW()"), "NOW");
        addTranslationRegex(MSSQL_SERVER, "N'(.*)'", "$1");

        addTranslation(POSTGRE_SQL, newArrayList("CURRENT_TIMESTAMP", "NOW()"), "NOW");
        addTranslationRegex(POSTGRE_SQL, "'(.*)'::.*", "$1");

        addTranslation(ORACLE, newArrayList("CURRENT_DATE", "SYSDATE"), "NOW");

        addTranslation(DB2, newArrayList("CURRENT DATE", "CURRENT TIME", "CURRENT TIMESTAMP"), "NOW");
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
    public boolean isSQLKeyword(String identifier, Identifiable identifiable) {
        return !(identifiable != null && identifiable instanceof Table) && getSQLKeywords().contains(identifier);
    }

    @Override
    public boolean supportsTransactionIsolation(int level) {
        return newArrayList(
                WRITE_COMMITTED, CONSISTENT_READ, TRANSACTION_READ_COMMITTED, TRANSACTION_SERIALIZABLE
        ).contains(level);
    }

    @Override
    public boolean supportsSequence() {
        return true;
    }

    @Override
    public String getSequenceStartWith(Long startWith) {
        return startWith != null ? "START WITH " + startWith : null;
    }

    @Override
    public String getSequenceIncrementBy(Long incrementBy) {
        return null;
    }

    @Override
    public String getSequenceMinValue(Long minValue) {
        return null;
    }

    @Override
    public String getSequenceMaxValue(Long maxValue) {
        return null;
    }

    @Override
    public String getSequenceCycle(boolean cycle) {
        return null;
    }

    @Override
    public String getSequenceCache(Integer cache) {
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

    @Override
    public boolean supportsDropConstraints() {
        return false;
    }

    @Override
    public boolean supportsDropIndexOnTable() {
        return true;
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
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean supportsLimitParameters() {
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