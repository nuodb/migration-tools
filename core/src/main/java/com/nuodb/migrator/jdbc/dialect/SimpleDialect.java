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
import com.nuodb.migrator.jdbc.metadata.ColumnTrigger;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Identifiable;
import com.nuodb.migrator.jdbc.metadata.ReferenceAction;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.Trigger;
import com.nuodb.migrator.jdbc.metadata.TriggerEvent;
import com.nuodb.migrator.jdbc.metadata.TriggerTime;
import com.nuodb.migrator.jdbc.query.QueryLimit;
import com.nuodb.migrator.jdbc.metadata.resolver.SimpleServiceResolverAware;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeAdapter;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.jdbc.type.JdbcTypeName;
import com.nuodb.migrator.jdbc.type.JdbcTypeNameMap;
import com.nuodb.migrator.jdbc.type.JdbcTypeOptions;
import com.nuodb.migrator.jdbc.type.JdbcTypeRegistry;
import com.nuodb.migrator.jdbc.type.JdbcTypeValue;
import com.nuodb.migrator.jdbc.type.JdbcValueAccessProvider;
import com.nuodb.migrator.jdbc.type.SimpleJdbcValueAccessProvider;
import com.nuodb.migrator.jdbc.type.jdbc4.Jdbc4TypeRegistry;
import org.apache.commons.lang3.text.translate.LookupTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.dialect.DialectUtils.NULL;
import static com.nuodb.migrator.jdbc.dialect.IdentifierNormalizers.NOOP;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.ALWAYS;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static java.lang.String.valueOf;
import static java.sql.Connection.*;
import static java.sql.Types.*;
import static java.sql.Types.DECIMAL;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

/**
 * @author Sergey Bushik
 */
public class SimpleDialect extends SimpleServiceResolverAware<Dialect> implements Dialect {

    private static final Pattern ALLOWED_IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z0-9_]*");
    private static final ScriptEscapeUtils SCRIPT_ESCAPE_UTILS = new ScriptEscapeUtils(new LookupTranslator(
            new String[][] { { "\0", "\\0" }, { "'", "''" }, { "\"", "\\\"" }, { "\\", "\\\\" } }));

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private IdentifierNormalizer identifierNormalizer = NOOP;
    private IdentifierQuoting identifierQuoting = ALWAYS;
    private Map<DatabaseInfo, JdbcTypeNameMap> jdbcTypeNameMaps = new HashMap<DatabaseInfo, JdbcTypeNameMap>();
    private JdbcTypeRegistry jdbcTypeRegistry = new Jdbc4TypeRegistry();
    private TranslationManager translationManager = new TranslationManager();
    private ScriptEscapeUtils scriptEscapeUtils = SCRIPT_ESCAPE_UTILS;
    private DatabaseInfo databaseInfo;

    public SimpleDialect(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;

        initJdbcTypes();
        initJdbcTypeNames();
        initTranslations();
    }

    protected void initJdbcTypes() {
    }

    protected void initJdbcTypeNames() {
        addJdbcTypeName(Types.BIT, "BIT");
        addJdbcTypeName(Types.BOOLEAN, "BOOLEAN");
        addJdbcTypeName(Types.TINYINT, "TINYINT");
        addJdbcTypeName(Types.SMALLINT, "SMALLINT");
        addJdbcTypeName(Types.INTEGER, "INTEGER");
        addJdbcTypeName(Types.BIGINT, "BIGINT");
        addJdbcTypeName(REAL, "REAL");
        addJdbcTypeName(FLOAT, "FLOAT");
        addJdbcTypeName(DOUBLE, "DOUBLE");
        addJdbcTypeName(NUMERIC, "NUMERIC({P},{S})");
        addJdbcTypeName(DECIMAL, "DECIMAL({P},{S})");

        addJdbcTypeName(Types.DATE, "DATE");
        addJdbcTypeName(Types.TIME, "TIME");
        addJdbcTypeName(Types.TIMESTAMP, "TIMESTAMP");

        addJdbcTypeName(Types.BLOB, "BLOB");

        addJdbcTypeName(Types.CHAR, "CHAR({N})");
        addJdbcTypeName(Types.NCHAR, "NCHAR({N})");

        addJdbcTypeName(Types.VARCHAR, "VARCHAR({N})");
        addJdbcTypeName(Types.NVARCHAR, "NVARCHAR({N})");

        addJdbcTypeName(Types.LONGVARCHAR, "VARCHAR({N})");
        addJdbcTypeName(Types.LONGNVARCHAR, "NVARCHAR({N})");

        addJdbcTypeName(Types.CLOB, "CLOB");
        addJdbcTypeName(Types.NCLOB, "NCLOB");

        addJdbcTypeName(BINARY, "BINARY({N})");
        addJdbcTypeName(VARBINARY, "VARBINARY({N})");
        addJdbcTypeName(LONGVARBINARY, "VARBINARY({N})");
    }

    protected void initTranslations() {
    }

    protected void addTranslation(DatabaseInfo sourceDatabaseInfo, String sourceScript, String targetScript) {
        getTranslationManager().addTranslation(sourceDatabaseInfo, sourceScript, getDatabaseInfo(), targetScript);
    }

    protected void addTranslation(DatabaseInfo sourceDatabaseInfo, Collection<String> sourceScripts,
            String targetScript) {
        getTranslationManager().addTranslations(sourceDatabaseInfo, sourceScripts, getDatabaseInfo(), targetScript);
    }

    protected void addTranslationRegex(DatabaseInfo sourceDatabaseInfo, String sourceScript, String targetScript) {
        getTranslationManager().addTranslationRegex(sourceDatabaseInfo, sourceScript, getDatabaseInfo(), targetScript);
    }

    protected void addTranslationPattern(DatabaseInfo sourceDatabaseInfo, Pattern sourceScript, String targetScript) {
        getTranslationManager().addTranslationPattern(sourceDatabaseInfo, sourceScript, getDatabaseInfo(),
                targetScript);
    }

    protected void addTranslator(Translator translator) {
        getTranslationManager().addTranslator(translator);
    }

    @Override
    public String getIdentifier(String identifier, Identifiable identifiable) {
        if (identifier == null) {
            return null;
        }
        identifier = getIdentifierNormalizer().normalizeIdentifier(identifier, identifiable, this);
        boolean quoting = getIdentifierQuoting().isQuotingIdentifier(identifier, identifiable, this);
        return quoting ? quote(identifier) : identifier;
    }

    @Override
    public Integer getMaxOpenCursors(Connection connection) throws SQLException {
        return null;
    }

    @Override
    public boolean isQuotingIdentifier(String identifier, Identifiable identifiable) {
        return !isAllowedIdentifier(identifier, identifiable) || isSQLKeyword(identifier, identifiable);
    }

    @Override
    public boolean isAllowedIdentifier(String identifier, Identifiable identifiable) {
        return ALLOWED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    public boolean isSQLKeyword(String identifier, Identifiable identifiable) {
        return getSQLKeywords().contains(identifier);
    }

    @Override
    public JdbcTypeDesc getJdbcTypeAlias(int typeCode, String typeName) {
        return getJdbcTypeRegistry().getJdbcTypeAlias(typeCode, typeName);
    }

    @Override
    public String quote(String value) {
        return openQuote() + value + closeQuote();
    }

    @Override
    public String normalizeIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String openQuote() {
        return valueOf('"');
    }

    @Override
    public String closeQuote() {
        return valueOf('"');
    }

    @Override
    public Script translate(String script, Session session) {
        return translate(script, session, null);
    }

    @Override
    public Script translate(String script, Session session, Map<Object, Object> context) {
        return translate(new SimpleScript(script), session, context);
    }

    @Override
    public Script translate(Script script, Session session) {
        return translate(script, session, null);
    }

    @Override
    public Script translate(Script script, Session session, Map<Object, Object> context) {
        return getTranslationManager().translate(script, this, session, context);
    }

    @Override
    public String getInlineColumnTrigger(Session session, ColumnTrigger trigger) {
        return null;
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
    public boolean supportsCreateMultipleIndexes() {
        return false;
    }

    @Override
    public void setSessionTimeZone(Connection connection, TimeZone timeZone) throws SQLException {
    }

    @Override
    public boolean supportsTransactionIsolation(int level) {
        boolean supports = false;
        switch (level) {
        case TRANSACTION_NONE:
        case TRANSACTION_READ_UNCOMMITTED:
        case TRANSACTION_READ_COMMITTED:
        case TRANSACTION_REPEATABLE_READ:
        case TRANSACTION_SERIALIZABLE:
            supports = true;
            break;
        }
        return supports;
    }

    @Override
    public void setTransactionIsolation(Connection connection, int[] levels) throws SQLException {
        if (levels != null) {
            for (int level : levels) {
                if (supportsTransactionIsolation(level)) {
                    connection.setTransactionIsolation(level);
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
        return getJdbcTypeNameMap(getDatabaseInfo());
    }

    @Override
    public JdbcTypeRegistry getJdbcTypeRegistry() {
        return jdbcTypeRegistry;
    }

    @Override
    public JdbcValueAccessProvider getJdbcValueAccessProvider() {
        return new SimpleJdbcValueAccessProvider(getJdbcTypeRegistry());
    }

    @Override
    public JdbcTypeNameMap getJdbcTypeNameMap(DatabaseInfo databaseInfo) {
        JdbcTypeNameMap targetJdbcTypeNameMap = jdbcTypeNameMaps.get(databaseInfo);
        if (targetJdbcTypeNameMap == null) {
            DatabaseInfo targetDatabaseInfo = null;
            for (Map.Entry<DatabaseInfo, JdbcTypeNameMap> entry : jdbcTypeNameMaps.entrySet()) {
                if (!entry.getKey().isAssignable(databaseInfo)) {
                    continue;
                }
                if (targetDatabaseInfo == null) {
                    targetDatabaseInfo = entry.getKey();
                    targetJdbcTypeNameMap = entry.getValue();
                } else if (targetDatabaseInfo.compareTo(entry.getKey()) >= 0) {
                    targetJdbcTypeNameMap = entry.getValue();
                }
            }
        }
        if (targetJdbcTypeNameMap == null) {
            jdbcTypeNameMaps.put(databaseInfo, targetJdbcTypeNameMap = new JdbcTypeNameMap());
        }
        return targetJdbcTypeNameMap;
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
    public TranslationManager getTranslationManager() {
        return translationManager;
    }

    @Override
    public ScriptEscapeUtils getScriptEscapeUtils() {
        return scriptEscapeUtils;
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
    public boolean supportsDropPrimaryKey() {
        return true;
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
    public boolean supportsRowCount(Table table, Column column, String filter, RowCountType rowCountType) {
        return rowCountType == EXACT;
    }

    @Override
    public RowCountHandler createRowCountHandler(Table table, Column column, String filter, RowCountType rowCountType) {
        return new SimpleTableRowCountHandler(this, table, column, filter, rowCountType);
    }

    @Override
    public boolean addScriptsInCreateTable(Table table) {
        return true;
    }

    @Override
    public boolean addConstraintsInCreateTable() {
        return false;
    }

    @Override
    public boolean supportInlineColumnTrigger(Session Session, ColumnTrigger trigger) {
        return false;
    }

    /**
     * Supports LIMIT {row count} syntax.
     *
     * @return true if limit query syntax is supported.
     */
    @Override
    public boolean supportsLimit() {
        return false;
    }

    /**
     * Supports LIMIT {offset, row count} syntax.
     *
     * @return true if limit with offset syntax is supported.
     */
    @Override
    public boolean supportsLimitOffset() {
        return supportsLimit();
    }

    @Override
    public boolean supportsLimitParameters() {
        return false;
    }

    @Override
    public boolean supportsCatalogs() {
        return false;
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public LimitHandler createLimitHandler(String query, QueryLimit queryLimit) {
        return new SimpleLimitHandler(this, query, queryLimit);
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
    public String getCheckClause(String clause) {
        return clause != null ? "(" + quoteScript(clause) + ")" : null;
    }

    @Override
    public String getUseSchema(String schema) {
        return getUseSchema(schema, false);
    }

    @Override
    public String getUseSchema(String schema, boolean normalize) {
        return "USE " + (normalize ? getIdentifier(schema, null) : schema);
    }

    @Override
    public String getDropSchema(String schema) {
        return getDropSchema(schema, false);
    }

    @Override
    public String getDropSchema(String schema, boolean normalize) {
        return "DROP SCHEMA " + (normalize ? getIdentifier(schema, null) : schema) + " CASCADE IF EXISTS";
    }

    @Override
    public String getTriggerActive(boolean active) {
        return null;
    }

    @Override
    public String getDefaultValue(Column column, Session session) {
        return getDefaultValue(column, translate(new ColumnScript(column), session));
    }

    protected String getDefaultValue(Column column, Script script) {
        String defaultValue = script != null ? script.getScript() : Column.getDefaultValue(column);
        boolean literal = script != null && script.isLiteral();
        return getDefaultValue(column, defaultValue, literal);
    }

    private String getDefaultValue(Column column, String script, boolean literal) {
        return isDefaultValueQuoted(column, script, literal) ? getDefaultValueQuoted(column, script, literal)
                : getDefaultValueNotQuoted(column, script, literal);
    }

    protected boolean isDefaultValueQuoted(Column column, String script, boolean literal) {
        return equalsIgnoreCase(script, NULL) ? false : script != null && !literal;
    }

    protected String getDefaultValueQuoted(Column column, String script, boolean literal) {
        if (script == null) {
            return null;
        }
        String defaultValue = script;
        boolean opening = false;
        if (script.startsWith("'")) {
            defaultValue = script.substring(1);
            opening = true;
        }
        if (opening && defaultValue.endsWith("'")) {
            defaultValue = defaultValue.substring(0, defaultValue.length() - 1);
        }
        defaultValue = getScriptEscapeUtils().escapeDefaultValue(defaultValue);
        return "'" + defaultValue + "'";
    }

    protected String getDefaultValueNotQuoted(Column column, String script, boolean literal) {
        // return NULL in uppercase
        return equalsIgnoreCase(script, NULL) ? NULL : script;
    }

    protected String quoteScript(String script) {
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
    public String getTriggerOn(Table table) {
        return "ON";
    }

    @Override
    public String getTriggerTime(TriggerTime triggerTime) {
        return triggerTime != null ? triggerTime.toString() : null;
    }

    @Override
    public String getTriggerEvent(TriggerEvent triggerEvent) {
        return triggerEvent != null ? triggerEvent.toString() : null;
    }

    @Override
    public String getTriggerBegin(Trigger trigger) {
        return "BEGIN";
    }

    @Override
    public String getTriggerBody(Trigger trigger, Session session) {
        Script script = translate(new TriggerScript(trigger), session);
        return script != null ? script.getScript() : trigger.getTriggerBody();
    }

    @Override
    public String getTriggerEnd(Trigger trigger) {
        return "END";
    }

    @Override
    public String getSequenceStartWith(Number startWith) {
        return startWith != null ? "START WITH " + startWith : null;
    }

    @Override
    public String getSequenceIncrementBy(Number incrementBy) {
        return incrementBy != null ? "INCREMENT BY " + incrementBy : null;
    }

    @Override
    public String getSequenceMinValue(Number minValue) {
        return minValue != null ? "MINVALUE " + minValue : "NO MINVALUE";
    }

    @Override
    public String getSequenceMaxValue(Number maxValue) {
        return maxValue != null ? "MAXVALUE " + maxValue : "NO MAXVALUE";
    }

    @Override
    public String getSequenceCycle(boolean cycle) {
        return cycle ? "CYCLE" : "NO CYCLE";
    }

    @Override
    public String getSequenceCache(Number cache) {
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
    public void setFetchMode(Statement statement, FetchMode fetchMode) throws SQLException {
        statement.setFetchSize(fetchMode.isStream() ? fetchMode.getFetchSize() : 0);
    }

    @Override
    public boolean supportsDropConstraints() {
        return true;
    }

    @Override
    public boolean supportsIfExistsBeforeDropTrigger() {
        return false;
    }

    @Override
    public boolean supportsIfExistsAfterDropTrigger() {
        return false;
    }

    @Override
    public boolean requiresTableLockForDDL() {
        return false;
    }

    @Override
    public String getTypeName(DatabaseInfo databaseInfo, JdbcType jdbcType) {
        String typeName = getTypeName(getJdbcTypeNameMap(databaseInfo), jdbcType);
        if (typeName == null) {
            typeName = getTypeName(getJdbcTypeNameMap(), jdbcType);
        }
        return typeName;
    }

    protected void addJdbcType(JdbcTypeValue type) {
        getJdbcTypeRegistry().addJdbcType(type);
    }

    protected void addJdbcTypeAdapter(JdbcTypeAdapter typeAdapter) {
        getJdbcTypeRegistry().addJdbcTypeAdapter(typeAdapter);
    }

    protected void addJdbcTypeAlias(JdbcTypeDesc jdbcTypeDesc, int typeCodeAlias) {
        getJdbcTypeRegistry().addJdbcTypeAlias(jdbcTypeDesc, typeCodeAlias);
    }

    protected void addJdbcTypeAlias(int typeCode, String typeName, int typeCodeAlias) {
        getJdbcTypeRegistry().addJdbcTypeAlias(typeCode, typeName, typeCodeAlias);
    }

    protected void addJdbcTypeName(int typeCode, String typeName) {
        getJdbcTypeNameMap().addJdbcTypeName(typeCode, typeName);
    }

    protected void addJdbcTypeName(int typeCode, JdbcTypeOptions jdbcTypeOptions, String typeName) {
        getJdbcTypeNameMap().addJdbcTypeName(typeCode, jdbcTypeOptions, typeName);
    }

    public void addJdbcTypeName(JdbcTypeName jdbcTypeName, int priority) {
        getJdbcTypeNameMap().addJdbcTypeName(jdbcTypeName, priority);
    }

    public void addJdbcTypeName(JdbcTypeName jdbcTypeName) {
        getJdbcTypeNameMap().addJdbcTypeName(jdbcTypeName);
    }

    protected void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, String typeName) {
        getJdbcTypeNameMap().addJdbcTypeName(jdbcTypeDesc, typeName);
    }

    protected void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, String typeName, int priority) {
        getJdbcTypeNameMap().addJdbcTypeName(jdbcTypeDesc, typeName, priority);
    }

    protected void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, JdbcTypeOptions jdbcTypeOptions, String typeName) {
        getJdbcTypeNameMap().addJdbcTypeName(jdbcTypeDesc, jdbcTypeOptions, typeName);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, int typeCode, String typeName) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(typeCode, typeName);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, int typeCode, String typeName,
            JdbcTypeOptions jdbcTypeOptions) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(typeCode, jdbcTypeOptions, typeName);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, JdbcType jdbcType, String typeName) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(jdbcType, typeName);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeDesc jdbcTypeDesc, String typeName) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(jdbcTypeDesc, typeName);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeDesc jdbcTypeDesc, String typeName,
            int priority) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(jdbcTypeDesc, typeName, priority);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeDesc jdbcTypeDesc, String typeName,
            JdbcTypeOptions jdbcTypeOptions) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(jdbcTypeDesc, jdbcTypeOptions, typeName);
    }

    public void addJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeName jdbcTypeName) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(jdbcTypeName);
    }

    public void addJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeName jdbcTypeName, int priority) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(jdbcTypeName, priority);
    }

    protected String getTypeName(JdbcTypeNameMap jdbcTypeNameMap, JdbcType jdbcType) {
        String typeName = jdbcTypeNameMap.getTypeName(jdbcType);
        if (typeName == null) {
            typeName = jdbcTypeNameMap.getTypeName(jdbcType.withTypeName(null));
        }
        return typeName;
    }

    protected void removeJdbcTypeName(int typeCode) {
        getJdbcTypeNameMap().removeJdbcTypeName(typeCode);
    }

    protected void removeJdbcTypeName(JdbcTypeDesc typeDesc) {
        getJdbcTypeNameMap().removeJdbcTypeName(typeDesc);
    }

    protected void removeJdbcTypeName(DatabaseInfo databaseInfo, int typeCode) {
        getJdbcTypeNameMap(databaseInfo).removeJdbcTypeName(typeCode);
    }

    protected void removeJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeDesc typeDesc) {
        getJdbcTypeNameMap(databaseInfo).removeJdbcTypeName(typeDesc);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SimpleDialect that = (SimpleDialect) o;
        if (identifierNormalizer != null ? !identifierNormalizer.equals(that.identifierNormalizer)
                : that.identifierNormalizer != null)
            return false;
        if (jdbcTypeNameMaps != null ? !jdbcTypeNameMaps.equals(that.jdbcTypeNameMaps) : that.jdbcTypeNameMaps != null)
            return false;
        if (jdbcTypeRegistry != null ? !jdbcTypeRegistry.equals(that.jdbcTypeRegistry) : that.jdbcTypeRegistry != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return databaseInfo != null ? databaseInfo.hashCode() : 0;
    }
}
