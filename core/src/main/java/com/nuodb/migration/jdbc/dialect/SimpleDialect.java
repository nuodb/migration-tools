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

import com.nuodb.migration.jdbc.metadata.HasIdentifier;
import com.nuodb.migration.jdbc.metadata.ReferenceAction;
import com.nuodb.migration.jdbc.resolve.DatabaseInfo;
import com.nuodb.migration.jdbc.resolve.DatabaseServiceResolver;
import com.nuodb.migration.jdbc.resolve.SimpleDatabaseServiceResolverAware;
import com.nuodb.migration.jdbc.type.*;
import com.nuodb.migration.jdbc.type.jdbc4.Jdbc4TypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.valueOf;
import static java.sql.Connection.*;

/**
 * @author Sergey Bushik
 */
public class SimpleDialect extends SimpleDatabaseServiceResolverAware<Dialect> implements Dialect {

    private static final Pattern ALLOWED_IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z0-9_]*");

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private ScriptTranslationManager scriptTranslationManager = new ScriptTranslationManager();
    private JdbcTypeNameMap jdbcTypeNameMap = new JdbcTypeNameMap();
    private JdbcTypeRegistry jdbcTypeRegistry = new Jdbc4TypeRegistry();
    private IdentifierNormalizer identifierNormalizer = IdentifierNormalizers.noop();
    private IdentifierQuoting identifierQuoting = IdentifierQuotings.always();
    private ScriptEscapeUtils scriptEscapeUtils = new ScriptEscapeUtils();
    private DatabaseInfo databaseInfo;

    public SimpleDialect(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;

        addTypeName(Types.BIT, "BIT");
        addTypeName(Types.BOOLEAN, "BOOLEAN");
        addTypeName(Types.TINYINT, "TINYINT");
        addTypeName(Types.SMALLINT, "SMALLINT");
        addTypeName(Types.INTEGER, "INTEGER");
        addTypeName(Types.BIGINT, "BIGINT");
        addTypeName(Types.FLOAT, "FLOAT({P})");
        addTypeName(Types.NUMERIC, "NUMERIC({P},{S})");
        addTypeName(Types.DECIMAL, "DECIMAL({P},{S})");
        addTypeName(Types.REAL, "REAL");

        addTypeName(Types.DATE, "DATE");
        addTypeName(Types.TIME, "TIME");
        addTypeName(Types.TIMESTAMP, "TIMESTAMP");

        addTypeName(Types.BLOB, "BLOB");

        addTypeName(Types.CHAR, "CHAR({N})");
        addTypeName(Types.NCHAR, "NCHAR({N})");

        addTypeName(Types.VARCHAR, "VARCHAR({N})");
        addTypeName(Types.NVARCHAR, "NVARCHAR({N})");

        addTypeName(Types.LONGVARCHAR, "VARCHAR({N})");
        addTypeName(Types.LONGNVARCHAR, "NVARCHAR({N})");

        addTypeName(Types.CLOB, "CLOB");
        addTypeName(Types.NCLOB, "NCLOB");
    }

    @Override
    public String getIdentifier(String identifier, HasIdentifier hasIdentifier) {
        if (identifier == null) {
            return null;
        }
        identifier = getIdentifierNormalizer().normalizeIdentifier(identifier, hasIdentifier, this);
        boolean quoting = getIdentifierQuoting().isQuotingIdentifier(identifier, hasIdentifier, this);
        return quoting ? quote(identifier) : identifier;
    }

    protected boolean isQuotingIdentifier(String identifier, HasIdentifier hasIdentifier) {
        return !isAllowedIdentifier(identifier, hasIdentifier) || isSQLKeyword(identifier, hasIdentifier);
    }

    protected boolean isAllowedIdentifier(String identifier, HasIdentifier hasIdentifier) {
        return ALLOWED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    protected boolean isSQLKeyword(String identifier, HasIdentifier hasIdentifier) {
        return getSQLKeywords().contains(identifier);
    }

    protected String quote(String identifier) {
        return openQuote() + identifier + closeQuote();
    }

    protected String normalizeIdentifier(String identifier) {
        return identifier;
    }

    protected String openQuote() {
        return valueOf('"');
    }

    protected String closeQuote() {
        return valueOf('"');
    }

    protected String getScriptTranslation(String sourceScript, Dialect sourceDialect) {
        Script targetScript = getScriptTranslationManager().getScriptTranslation(
                sourceDialect, this, new SimpleScript(sourceScript));
        return targetScript != null ? targetScript.getScript() : null;
    }

    protected void addScriptTranslation(DatabaseInfo sourceDatabaseInfo, String sourceScript, String targetScript) {
        getScriptTranslationManager().addScriptTranslation(
                new ScriptTranslation(sourceDatabaseInfo, this,
                        new SimpleScript(sourceScript), new SimpleScript(targetScript)));
    }

    @Override
    public String getNoColumnsInsert() {
        return "VALUES ()";
    }

    @Override
    public String getNullColumnString() {
        return "";
    }

    @Override
    public boolean supportsUniqueInCreateTable() {
        return true;
    }

    @Override
    public boolean supportsNotNullUnique() {
        return true;
    }

    @Override
    public boolean supportsSessionTimeZone() {
        return false;
    }

    @Override
    public boolean supportsColumnCheck() {
        return true;
    }

    @Override
    public void setSessionTimeZone(Connection connection, TimeZone timeZone) throws SQLException {
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int transactionIsolationLevel) {
        return newArrayList(
                TRANSACTION_NONE,
                TRANSACTION_READ_UNCOMMITTED,
                TRANSACTION_READ_COMMITTED,
                TRANSACTION_REPEATABLE_READ,
                TRANSACTION_SERIALIZABLE).contains(transactionIsolationLevel);
    }

    @Override
    public void setTransactionIsolationLevel(Connection connection,
                                             int[] transactionIsolationLevels) throws SQLException {
        if (transactionIsolationLevels != null) {
            for (int transactionIsolationLevel : transactionIsolationLevels) {
                if (supportsTransactionIsolationLevel(transactionIsolationLevel)) {
                    connection.setTransactionIsolation(transactionIsolationLevel);
                    return;
                }
            }
        }
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }

    @Override
    public SQLKeywords getSQLKeywords() {
        return SQLKeywords.SQL_2003_KEYWORDS;
    }

    @Override
    public JdbcTypeNameMap getJdbcTypeNameMap() {
        return jdbcTypeNameMap;
    }

    @Override
    public JdbcTypeRegistry getJdbcTypeRegistry() {
        return jdbcTypeRegistry;
    }

    @Override
    public IdentifierNormalizer getIdentifierNormalizer() {
        return identifierNormalizer;
    }

    @Override
    public void setIdentifierNormalizer(IdentifierNormalizer identifierNormalizer) {
        this.identifierNormalizer = identifierNormalizer;
    }

    @Override
    public IdentifierQuoting getIdentifierQuoting() {
        return identifierQuoting;
    }

    @Override
    public void setIdentifierQuoting(IdentifierQuoting identifierQuoting) {
        this.identifierQuoting = identifierQuoting;
    }

    @Override
    public ScriptTranslationManager getScriptTranslationManager() {
        return scriptTranslationManager;
    }

    @Override
    public void setScriptTranslationManager(ScriptTranslationManager scriptTranslationManager) {
        this.scriptTranslationManager = scriptTranslationManager;
    }

    @Override
    public void setDatabaseServiceResolver(DatabaseServiceResolver<Dialect> databaseServiceResolver) {
        super.setDatabaseServiceResolver(databaseServiceResolver);
        scriptTranslationManager.setDatabaseServiceResolver(databaseServiceResolver);
    }

    @Override
    public ScriptEscapeUtils getScriptEscapeUtils() {
        return scriptEscapeUtils;
    }

    @Override
    public void setScriptEscapeUtils(ScriptEscapeUtils scriptEscapeUtils) {
        this.scriptEscapeUtils = scriptEscapeUtils;
    }

    @Override
    public boolean supportsDropIndexIfExists() {
        return false;
    }

    @Override
    public boolean supportsDropIndexOnTable() {
        return false;
    }

    @Override
    public boolean supportsStatementWithTimezone() {
        return true;
    }

    @Override
    public boolean supportsDropSequenceIfExists() {
        return false;
    }

    @Override
    public boolean supportsSequence() {
        return false;
    }

    @Override
    public boolean supportsNegativeScale() {
        return false;
    }

    @Override
    public String getColumnComment(String comment) {
        return "";
    }

    @Override
    public String getTableComment(String comment) {
        return "";
    }

    @Override
    public String getIdentityColumn(String sequence) {
        return "";
    }

    @Override
    public String getCheckClause(String checkClause) {
        checkClause = getScriptQuotation(checkClause);
        if (!checkClause.startsWith("(") && !checkClause.endsWith(")")) {
            return "(" + checkClause + ")";
        } else {
            return checkClause;
        }
    }

    @Override
    public String getDefaultValue(int typeCode, String defaultValue, Dialect dialect) {
        if (defaultValue == null) {
            return null;
        }
        defaultValue = getScriptTranslation(defaultValue, dialect);
        String defaultValueUnquoted = defaultValue;
        boolean opening = false;
        if (defaultValue.startsWith("'")) {
            defaultValueUnquoted = defaultValue.substring(1);
            opening = true;
        }
        if (opening && defaultValueUnquoted.endsWith("'")) {
            defaultValueUnquoted = defaultValueUnquoted.substring(0, defaultValueUnquoted.length() - 1);
        }
        defaultValueUnquoted = getScriptEscapeUtils().escapeDefaultValue(defaultValueUnquoted);
        return "'" + defaultValueUnquoted + "'";
    }

    protected String getScriptQuotation(String script) {
        if (script == null) {
            return null;
        }
        StringTokenizer tokens = new StringTokenizer(script, "+*/-=<>'`\"[](), \t\n\r\f", true);
        StringBuilder result = new StringBuilder();
        while (tokens.hasMoreTokens()) {
            String token = tokens.nextToken();
            boolean quote = false;
            String closeQuote = null;
            if ("\"".equals(token)) {
                quote = true;
                closeQuote = "\"";
            } else if ("[".equals(token)) {
                quote = true;
                closeQuote = "]";
            } else if ("`".equals(token)) {
                quote = true;
                closeQuote = "`";
            }
            if (quote) {
                String identifier = null;
                for (String next = tokens.nextToken(); !closeQuote.equals(next); next = tokens.nextToken()) {
                    identifier = identifier == null ? next : identifier + next;
                }
                if (identifier != null) {
                    token = getIdentifier(identifier, null);
                }
            }
            result.append(token);
        }
        return result.toString();
    }

    @Override
    public String getUpdateAction(ReferenceAction updateAction) {
        return null;
    }

    @Override
    public String getSequenceStartWith(Long startWith) {
        return startWith != null ? "START WITH " + startWith : null;
    }

    @Override
    public String getSequenceIncrementBy(Long incrementBy) {
        return incrementBy != null ? "INCREMENT BY " + incrementBy : null;
    }

    @Override
    public String getSequenceMinValue(Long minValue) {
        return minValue != null ? "MINVALUE " + minValue : "NO MINVALUE";
    }

    @Override
    public String getSequenceMaxValue(Long maxValue) {
        return maxValue != null ? "MAXVALUE " + maxValue : "NO MAXVALUE";
    }

    @Override
    public String getSequenceCycle(boolean cycle) {
        return cycle ? "CYCLE" : "NO CYCLE";
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
    public boolean supportsIfExistsBeforeDropTable() {
        return false;
    }

    @Override
    public boolean supportsIfExistsAfterDropTable() {
        return false;
    }

    @Override
    public String getCascadeConstraints() {
        return null;
    }

    @Override
    public String getDropForeignKey() {
        return "DROP CONSTRAINT";
    }

    @Override
    public String getDeleteAction(ReferenceAction deleteAction) {
        return null;
    }

    @Override
    public boolean supportsIndexInCreateTable() {
        return true;
    }

    @Override
    public boolean supportsTableCheck() {
        return true;
    }

    @Override
    public void setStreamResults(Statement statement, boolean streamResults) throws SQLException {
    }

    @Override
    public boolean supportsDropConstraints() {
        return true;
    }

    public void addTypeName(int typeCode, String typeName) {
        jdbcTypeNameMap.addTypeName(typeCode, typeName);
    }

    public void addTypeName(int typeCode, String typeName, JdbcTypeSpecifiers typeSpecifiers) {
        jdbcTypeNameMap.addTypeName(typeCode, typeName, typeSpecifiers);
    }

    public void addTypeName(JdbcTypeDesc typeDesc, String typeName) {
        jdbcTypeNameMap.addTypeName(typeDesc, typeName);
    }

    public void removeTypeName(int typeCode) {
        jdbcTypeNameMap.removeTypeName(typeCode);
    }

    public void removeTypeName(JdbcTypeDesc typeDesc) {
        jdbcTypeNameMap.removeTypeName(typeDesc);
    }

    public void addJdbcType(JdbcType type) {
        jdbcTypeRegistry.addJdbcType(type);
    }

    public void addJdbcTypeAdapter(JdbcTypeAdapter typeAdapter) {
        jdbcTypeRegistry.addJdbcTypeAdapter(typeAdapter);
    }

    public void addJdbcTypeDescAlias(int typeCode, String typeName, int typeCodeAlias) {
        jdbcTypeRegistry.addJdbcTypeDescAlias(typeCode, typeName, typeCodeAlias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleDialect that = (SimpleDialect) o;

        if (identifierNormalizer != null ? !identifierNormalizer.equals(
                that.identifierNormalizer) : that.identifierNormalizer != null) return false;
        if (jdbcTypeNameMap != null ? !jdbcTypeNameMap.equals(that.jdbcTypeNameMap) : that.jdbcTypeNameMap != null)
            return false;
        if (jdbcTypeRegistry != null ? !jdbcTypeRegistry.equals(that.jdbcTypeRegistry) : that.jdbcTypeRegistry != null)
            return false;
        if (logger != null ? !logger.equals(that.logger) : that.logger != null) return false;
        if (scriptTranslationManager != null ? !scriptTranslationManager.equals(
                that.scriptTranslationManager) : that.scriptTranslationManager != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return databaseInfo != null ? databaseInfo.hashCode() : 0;
    }
}
