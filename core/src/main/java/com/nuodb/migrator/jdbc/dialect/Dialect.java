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
import com.nuodb.migrator.jdbc.type.JdbcTypeNameMap;
import com.nuodb.migrator.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers;
import com.nuodb.migrator.jdbc.type.JdbcValueAccessProvider;

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

    boolean supportsTransactionIsolation(int transactionIsolationLevel);

    boolean supportsIfExistsBeforeDropTable();

    boolean supportsIfExistsAfterDropTable();

    boolean supportsDropIndexIfExists();

    boolean supportsDropIndexOnTable();

    boolean supportsDropConstraints();

    boolean supportsStatementWithTimezone();

    boolean supportsDropSequenceIfExists();

    boolean supportsSequence();

    boolean supportsNegativeScale();

    boolean supportsLimit();

    boolean supportsLimitOffset();

    boolean supportsLimitParameters();

    LimitHandler createLimitHandler(String query, QueryLimit queryLimit);

    boolean supportsRowCount(Table table, Column column, String filter, RowCountType rowCountType);

    RowCountHandler createRowCountHandler(Table table, Column column, String filter, RowCountType rowCountType);

    String getNullColumnString();

    String getNoColumnsInsert();

    String getCascadeConstraints();

    String getDropForeignKey();

    String getColumnComment(String comment);

    String getTableComment(String comment);

    String getIdentityColumn(String sequence);

    String getDefaultValue(int typeCode, String defaultValue, Dialect dialect);

    String getDeleteAction(ReferenceAction deleteAction);

    String getUpdateAction(ReferenceAction updateAction);

    String getSequenceStartWith(Long startWith);

    String getSequenceIncrementBy(Long incrementBy);

    String getSequenceMinValue(Long minValue);

    String getSequenceMaxValue(Long maxValue);

    String getSequenceCycle(boolean cycle);

    String getSequenceCache(Integer cache);

    String getSequenceOrder(boolean order);

    String getCheckClause(String clause);

    String getIdentifier(String identifier, Identifiable identifiable);

    void setSessionTimeZone(Connection connection, TimeZone timeZone) throws SQLException;

    void setStreamResults(Statement statement, boolean streamResults) throws SQLException;

    void setTransactionIsolation(Connection connection, int[] levels) throws SQLException;

    String openQuote();

    String closeQuote();

    String normalizeIdentifier(String identifier);

    boolean isAllowedIdentifier(String identifier, Identifiable identifiable);

    boolean isQuotingIdentifier(String identifier, Identifiable identifiable);

    boolean isSQLKeyword(String identifier, Identifiable identifiable);

    String getJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeDesc typeDesc, JdbcTypeSpecifiers typeSpecifiers);

    JdbcTypeDesc getJdbcTypeAlias(int typeCode, String typeName);

    String translateScript(String script, DatabaseInfo databaseInfo);

    Script translateScript(Script script);

    SQLKeywords getSQLKeywords();

    JdbcTypeNameMap getJdbcTypeNameMap();

    JdbcTypeNameMap getJdbcTypeNameMap(DatabaseInfo databaseInfo);

    JdbcTypeRegistry getJdbcTypeRegistry();

    JdbcValueAccessProvider getJdbcValueAccessProvider();

    IdentifierNormalizer getIdentifierNormalizer();

    DatabaseInfo getDatabaseInfo();

    void setIdentifierNormalizer(IdentifierNormalizer identifierNormalizer);

    IdentifierQuoting getIdentifierQuoting();

    void setIdentifierQuoting(IdentifierQuoting identifierQuoting);

    ScriptEscapeUtils getScriptEscapeUtils();

    void setScriptEscapeUtils(ScriptEscapeUtils scriptEscapeUtils);

    ScriptTranslationManager getScriptTranslationManager();

    void setScriptTranslationManager(ScriptTranslationManager scriptTranslationManager);
}

