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

import com.nuodb.migration.jdbc.metadata.Identifiable;
import com.nuodb.migration.jdbc.metadata.ReferenceAction;
import com.nuodb.migration.jdbc.metadata.Table;
import com.nuodb.migration.jdbc.resolve.DatabaseInfo;
import com.nuodb.migration.jdbc.type.JdbcTypeDesc;

import java.sql.Types;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migration.jdbc.type.JdbcTypeSpecifiers.newScale;
import static com.nuodb.migration.jdbc.type.JdbcTypeSpecifiers.newSize;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static org.apache.commons.lang3.StringUtils.isAllUpperCase;

/**
 * @author Sergey Bushik
 */
public class NuoDBDialect extends SimpleDialect {

    private static final Pattern ALLOWED_IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z_]+\\w*$");

    private static final int WRITE_COMMITTED = 5;
    private static final int CONSISTENT_READ = 7;

    public NuoDBDialect() {
        this(new DatabaseInfo("NuoDB"));
    }

    public NuoDBDialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);

        removeTypeName(Types.BIT);
        addTypeName(Types.BIT, "BOOLEAN", newSize(1));

        addTypeName(Types.TINYINT, "SMALLINT");
        addTypeName(Types.SMALLINT, "SMALLINT");
        addTypeName(Types.INTEGER, "INTEGER");
        addTypeName(Types.BIGINT, "BIGINT");

        addTypeName(Types.REAL, "REAL");
        addTypeName(Types.FLOAT, "FLOAT");
        addTypeName(Types.DOUBLE, "DOUBLE");
        addTypeName(Types.DECIMAL, "DECIMAL({P},{S})");

        addTypeName(Types.CHAR, "CHAR({N})");

        addTypeName(Types.VARCHAR, "VARCHAR({N})");
        addTypeName(Types.LONGVARCHAR, "VARCHAR({N})");

        addTypeName(Types.DATE, "DATE");
        addTypeName(Types.TIME, "TIME", newScale(0));
        addTypeName(Types.TIME, "TIME({S})");
        addTypeName(Types.TIMESTAMP, "TIMESTAMP({S})");
        addTypeName(Types.TIMESTAMP, "TIMESTAMP", newScale(0));

        addTypeName(Types.BINARY, "BINARY({N})");
        addTypeName(Types.VARBINARY, "VARBINARY({N})");
        addTypeName(Types.LONGVARBINARY, "VARBINARY({N})");

        addTypeName(Types.NULL, "NULL");
        addTypeName(Types.BLOB, "BLOB");
        addTypeName(Types.CLOB, "CLOB");
        addTypeName(Types.BOOLEAN, "BOOLEAN");

        addTypeName(Types.NCHAR, "NCHAR({N})");
        addTypeName(Types.NVARCHAR, "NVARCHAR({N})");
        addTypeName(Types.NCLOB, "NCLOB");
        addTypeName(new JdbcTypeDesc(Types.VARCHAR, "STRING"), "STRING");

        addJdbcType(NuoDBIntegerType.INSTANCE);
        addJdbcType(NuoDBBigIntType.INSTANCE);
        addJdbcType(NuoDBTimeType.INSTANCE);

        addScriptTranslation(new DatabaseInfo("MySQL"), "CURRENT_TIMESTAMP", "NOW");
    }

    @Override
    protected boolean isQuotingIdentifier(String identifier, Identifiable identifiable) {
        return super.isQuotingIdentifier(identifier, identifiable) || !isAllUpperCase(identifier);
    }

    @Override
    protected boolean isAllowedIdentifier(String identifier, Identifiable identifiable) {
        return ALLOWED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    @Override
    protected boolean isSQLKeyword(String identifier, Identifiable identifiable) {
        if (identifiable != null && identifiable instanceof Table) {
            return false;
        } else {
            return getSQLKeywords().contains(identifier);
        }
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int transactionIsolationLevel) {
        return newArrayList(
                WRITE_COMMITTED, CONSISTENT_READ,
                TRANSACTION_READ_COMMITTED, TRANSACTION_SERIALIZABLE).contains(transactionIsolationLevel);
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
        return "DROP CONSTRAINT FOREIGN";
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
}