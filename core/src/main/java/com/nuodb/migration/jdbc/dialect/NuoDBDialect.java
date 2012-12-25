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

import com.nuodb.migration.jdbc.metadata.ReferenceAction;

import java.sql.Types;

import static com.google.common.collect.Lists.newArrayList;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

/**
 * @author Sergey Bushik
 */
public class NuoDBDialect extends SQL2003Dialect {

    private static final int WRITE_COMMITTED = 5;
    private static final int CONSISTENT_READ = 7;

    public NuoDBDialect() {
        addJdbcType(NuoDBIntegerType.INSTANCE);
        addJdbcType(NuoDBBigIntType.INSTANCE);
        addJdbcType(NuoDBTimeType.INSTANCE);

        addJdbcTypeName(Types.BIT, "BOOLEAN", 1);
        removeJdbcTypeName(Types.BIT);

        addJdbcTypeName(Types.TINYINT, "SMALLINT");
        addJdbcTypeName(Types.SMALLINT, "SMALLINT");
        addJdbcTypeName(Types.INTEGER, "INTEGER");
        addJdbcTypeName(Types.BIGINT, "BIGINT");

        addJdbcTypeName(Types.REAL, "REAL");
        addJdbcTypeName(Types.FLOAT, "FLOAT");
        addJdbcTypeName(Types.DOUBLE, "DOUBLE");
        addJdbcTypeName(Types.DECIMAL, "DECIMAL({P},{S})");

        addJdbcTypeName(Types.CHAR, "CHAR", 1);
        addJdbcTypeName(Types.CHAR, "CHAR({N})");

        // addJdbcTypeName(Types.VARCHAR, "CHARACTER VARYING({N})");
        // addJdbcTypeName(Types.LONGVARCHAR, "CHARACTER VARYING({N})");
        addJdbcTypeName(Types.VARCHAR, "VARCHAR({N})");
        addJdbcTypeName(Types.LONGVARCHAR, "VARCHAR({N})");

        addJdbcTypeName(Types.DATE, "DATE");
        addJdbcTypeName(Types.TIME, "TIME({S})");
        addJdbcTypeName(Types.TIMESTAMP, "TIMESTAMP({S})");

        addJdbcTypeName(Types.BINARY, "BINARY({N})");

        // addJdbcTypeName(Types.VARBINARY, "BINARY VARYING({N})");
        // addJdbcTypeName(Types.LONGVARBINARY, "BINARY VARYING({N})");
        addJdbcTypeName(Types.VARBINARY, "VARBINARY({N})");
        addJdbcTypeName(Types.LONGVARBINARY, "VARBINARY({N})");

        addJdbcTypeName(Types.NULL, "NULL");

        // addJdbcTypeName(Types.BLOB, "BINARY LARGE OBJECT");
        // addJdbcTypeName(Types.CLOB, "CHARACTER LARGE OBJECT");
        // addJdbcTypeName(Types.BOOLEAN, "BOOLEAN");
        addJdbcTypeName(Types.BLOB, "BLOB");
        addJdbcTypeName(Types.CLOB, "CLOB");
        addJdbcTypeName(Types.BOOLEAN, "BOOLEAN");

        // addJdbcTypeName(Types.NCHAR, "NATIONAL CHARACTER");
        // addJdbcTypeName(Types.NVARCHAR, "NATIONAL CHARACTER VARYING({N})");
        // addJdbcTypeName(Types.NCLOB, "NATIONAL CHARACTER LARGE OBJECT");
        addJdbcTypeName(Types.NCHAR, "NCHAR", 1);
        addJdbcTypeName(Types.NCHAR, "NCHAR({N})");
        addJdbcTypeName(Types.NVARCHAR, "NVARCHAR({N})");
        addJdbcTypeName(Types.NCLOB, "NCLOB");
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

    // @Override
    // protected String normalize(String identifier) {
    //     return identifier.toUpperCase();
    // }

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
    public boolean supportsTableCheck() {
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