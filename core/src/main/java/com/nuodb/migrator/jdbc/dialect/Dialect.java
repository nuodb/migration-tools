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

import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.type.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
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

    boolean supportsCreateMultipleIndexes();

    boolean supportsIndexInCreateTable();

    boolean supportsTableCheck();

    boolean supportsTransactionIsolation(int transactionIsolationLevel);

    boolean supportsIfExistsBeforeDropTable();

    boolean supportsIfExistsAfterDropTable();

    boolean supportsDropIndexIfExists();

    boolean supportsDropIndexOnTable();

    boolean supportsDropConstraints();

    boolean supportsIfExistsBeforeDropTrigger();

    boolean supportsIfExistsAfterDropTrigger();

    boolean supportsStatementWithTimezone();

    boolean supportsDropSequenceIfExists();

    boolean supportsDropPrimaryKey();

    boolean supportsSequence();

    boolean supportsNegativeScale();

    boolean supportsLimit();

    boolean supportsLimitOffset();

    boolean supportsLimitParameters();

    boolean supportsCatalogs();

    boolean supportsSchemas();

    boolean supportInlineColumnTrigger(Session session, ColumnTrigger trigger);

    LimitHandler createLimitHandler(String query, QueryLimit queryLimit);

    boolean supportsRowCount(Table table, Column column, String filter, RowCountType rowCountType);

    RowCountHandler createRowCountHandler(Table table, Column column, String filter, RowCountType rowCountType);

    boolean addScriptsInCreateTable(Table table);

    boolean addConstraintsInCreateTable();

    String getNullColumnString();

    String getNoColumnsInsert();

    String getCascadeConstraints();

    String getDropForeignKey();

    String getColumnComment(String comment);

    String getTableComment(String comment);

    String getIdentityColumn(String sequence);

    String getDefaultValue(Column column, Session session);

    String getDeleteAction(ReferenceAction deleteAction);

    String getUpdateAction(ReferenceAction updateAction);

    String getTriggerOn(Table table);

    String getTriggerActive(boolean active);

    String getTriggerTime(TriggerTime triggerTime);

    String getTriggerEvent(TriggerEvent triggerEvent);

    String getTriggerBegin(Trigger trigger);

    String getTriggerBody(Trigger trigger, Session session);

    String getTriggerEnd(Trigger trigger);

    String getSequenceStartWith(Number startWith);

    String getSequenceIncrementBy(Number incrementBy);

    String getSequenceMinValue(Number minValue);

    String getSequenceMaxValue(Number maxValue);

    String getSequenceCycle(boolean cycle);

    String getSequenceCache(Number cache);

    String getSequenceOrder(boolean order);

    String getCheckClause(String clause);

    String getUseSchema(String schema);

    String getUseSchema(String schema, boolean normalize);

    String getDropSchema(String schema);

    String getDropSchema(String schema, boolean normalize);

    String getIdentifier(String identifier, Identifiable identifiable);

    Integer getMaxOpenCursors(Connection connection) throws SQLException;

    void setSessionTimeZone(Connection connection, TimeZone timeZone) throws SQLException;

    void setFetchMode(Statement statement, FetchMode fetchMode) throws SQLException;

    void setTransactionIsolation(Connection connection, int[] levels) throws SQLException;

    String quote(String value);

    String openQuote();

    String closeQuote();

    String normalizeIdentifier(String identifier);

    boolean isAllowedIdentifier(String identifier, Identifiable identifiable);

    boolean isQuotingIdentifier(String identifier, Identifiable identifiable);

    boolean isSQLKeyword(String identifier, Identifiable identifiable);

    String getTypeName(DatabaseInfo databaseInfo, JdbcType jdbcType);

    JdbcTypeDesc getJdbcTypeAlias(int typeCode, String typeName);

    Script translate(String script, Session session);

    Script translate(String script, Session session, Map<Object, Object> context);

    Script translate(Script script, Session session);

    Script translate(Script script, Session session, Map<Object, Object> context);

    String getInlineColumnTrigger(Session session, ColumnTrigger trigger);

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

    TranslationManager getTranslationManager();

    boolean requiresTableLockForDDL();
}
