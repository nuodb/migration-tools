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
import com.nuodb.migration.jdbc.type.JdbcTypeRegistry;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

/**
 * Vendor specific database dialect
 *
 * @author Sergey Bushik
 */
public interface Dialect {

    boolean supportsUniqueInCreateTable();

    boolean supportsNotNullUnique();

    boolean supportsSessionTimeZone();

    boolean supportsColumnCheck();

    boolean supportsIndexInCreateTable();

    boolean supportsTableCheck();

    boolean supportsTransactionIsolationLevel(int transactionIsolationLevel);

    boolean supportsIfExistsBeforeDropTable();

    boolean supportsIfExistsAfterDropTable();

    boolean supportsDropIndexIfExists();

    boolean supportsDropIndexOnTable();

    boolean supportsDropConstraints();

    boolean supportsStatementWithTimezone();

    boolean supportsDropSequenceIfExists();

    boolean supportsSequence();

    boolean supportsNegativeScale();

    String getIdentifier(String identifier);

    String getIdentifier(String identifier, boolean normalizeIdentifier);

    String getNullColumnString();

    String getNoColumnsInsert();

    String getCascadeConstraints();

    String getDropForeignKey();

    String getColumnComment(String comment);

    String getTableComment(String comment);

    String getIdentityColumn(String sequence);

    String getDefaultValue(int typeCode, String defaultValue);

    String getDeleteAction(ReferenceAction deleteAction);

    String getUpdateAction(ReferenceAction updateAction);

    String getSequenceStartWith(Long startWith);

    String getSequenceIncrementBy(Long incrementBy);

    String getSequenceMinValue(Long minValue);

    String getSequenceMaxValue(Long maxValue);

    String getSequenceCycle(boolean cycle);

    String getSequenceCache(Integer cache);

    String getSequenceOrder(boolean order);

    String getCheckClause(String checkClause);

    void setStreamResults(Statement statement, boolean streamResults) throws SQLException;

    void setSessionTimeZone(Connection connection, TimeZone timeZone) throws SQLException;

    void setTransactionIsolationLevel(Connection connection, int[] transactionIsolationLevels) throws SQLException;

    SQLKeywords getSQLKeywords();

    JdbcTypeRegistry getJdbcTypeRegistry();
}
