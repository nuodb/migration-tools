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

import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;
import com.nuodb.migrator.jdbc.resolve.ServiceResolver;
import com.nuodb.migrator.jdbc.resolve.SimpleServiceResolverAware;
import com.nuodb.migrator.jdbc.type.*;
import com.nuodb.migrator.jdbc.type.jdbc4.Jdbc4TypeRegistry;
import org.apache.commons.lang3.mutable.MutableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.dialect.IdentifierNormalizers.NOOP;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.ALWAYS;
import static com.nuodb.migrator.jdbc.dialect.RowCountType.EXACT;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.sql.Connection.*;

/**
 * @author Sergey Bushik
 */
public class SimpleDialect extends SimpleServiceResolverAware<Dialect> implements Dialect {

    private static final Pattern ALLOWED_IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z0-9_]*");

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private IdentifierNormalizer identifierNormalizer = NOOP;
    private IdentifierQuoting identifierQuoting = ALWAYS;
    private Map<DatabaseInfo, JdbcTypeNameMap> jdbcTypeNameMaps = new HashMap<DatabaseInfo, JdbcTypeNameMap>();
    private JdbcTypeRegistry jdbcTypeRegistry = new Jdbc4TypeRegistry();
    private ScriptTranslationManager scriptTranslationManager = new ScriptTranslationManager();
    private ScriptEscapeUtils scriptEscapeUtils = new ScriptEscapeUtils();
    private DatabaseInfo databaseInfo;

    public SimpleDialect(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;

        initJdbcTypes();
        initJdbcTypeNames();
        initScriptTranslations();
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
        addJdbcTypeName(Types.FLOAT, "FLOAT({P})");
        addJdbcTypeName(Types.NUMERIC, "NUMERIC({P},{S})");
        addJdbcTypeName(Types.DECIMAL, "DECIMAL({P},{S})");
        addJdbcTypeName(Types.REAL, "REAL");

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
    }

    protected void initScriptTranslations() {
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
    public JdbcTypeDesc getJdbcTypeDescAlias(int typeCode, String typeName) {
        return getJdbcTypeRegistry().getJdbcTypeDescAlias(typeCode, typeName);
    }

    protected String quote(String identifier) {
        return openQuote() + identifier + closeQuote();
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

    protected void addScriptTranslation(DatabaseInfo databaseInfo, String sourceScript, String targetScript) {
        getScriptTranslationManager().addScriptTranslation(
                new ScriptTranslation(databaseInfo, this,
                        new SimpleScript(sourceScript), new SimpleScript(targetScript)));
    }

    @Override
    public String getScriptTranslation(DatabaseInfo databaseInfo, String script) {
        Script targetScript = getScriptTranslationManager().getScriptTranslation(
                databaseInfo, getDatabaseInfo(), new SimpleScript(script));
        return targetScript != null ? targetScript.getScript() : null;
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
        return getJdbcTypeNameMap(getDatabaseInfo());
    }

    @Override
    public JdbcTypeRegistry getJdbcTypeRegistry() {
        return jdbcTypeRegistry;
    }

    @Override
    public JdbcTypeNameMap getJdbcTypeNameMap(DatabaseInfo databaseInfo) {
        JdbcTypeNameMap jdbcTypeNameMap = jdbcTypeNameMaps.get(databaseInfo);
        if (jdbcTypeNameMap == null) {
            jdbcTypeNameMaps.put(databaseInfo, jdbcTypeNameMap = new JdbcTypeNameMap());
        }
        return jdbcTypeNameMap;
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
    public void setServiceResolver(ServiceResolver<Dialect> serviceResolver) {
        super.setServiceResolver(serviceResolver);
        scriptTranslationManager.setServiceResolver(serviceResolver);
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
    public boolean supportsRowCount(Table table, RowCountType rowCountType) {
        return createRowCountQuery(table, rowCountType) != null;
    }

    @Override
    public RowCountValue getRowCount(Connection connection, Table table,
                                     RowCountType rowCountType) throws SQLException {
        RowCountQuery rowCountQuery = createRowCountQuery(table, rowCountType);
        if (rowCountQuery != null) {
            return executeRowCountQuery(connection, rowCountQuery);
        } else {
            throw new DialectException(format("Row count type is not supported %s", rowCountType));
        }
    }

    protected RowCountQuery createRowCountQuery(Table table, RowCountType rowCountType) {
        RowCountQuery rowCountQuery = null;
        switch (rowCountType) {
            case APPROX:
                rowCountQuery = createRowCountApproxQuery(table);
                break;
            case EXACT:
                rowCountQuery = createRowCountExactQuery(table);
                break;
        }
        return rowCountQuery;
    }

    protected RowCountQuery createRowCountApproxQuery(Table table) {
        return null;
    }

    protected RowCountQuery createRowCountExactQuery(Table table) {
        SelectQuery query = new SelectQuery();
        query.setQualifyNames(true);
        query.setDialect(this);
        query.addTable(table);
        PrimaryKey primaryKey = table.getPrimaryKey();
        Column column = null;
        if (primaryKey != null && size(primaryKey.getColumns()) > 0) {
            column = get(primaryKey.getColumns(), 0);
        }
        query.addColumn("COUNT(" + (column != null ? column.getName(this) : "*") + ")");

        RowCountQuery rowCountQuery = new RowCountQuery();
        rowCountQuery.setRowCountType(EXACT);
        rowCountQuery.setQuery(query);
        rowCountQuery.setColumn(column);
        return rowCountQuery;
    }

    protected RowCountValue executeRowCountQuery(Connection connection,
                                                 final RowCountQuery rowCountQuery) throws SQLException {
        final MutableObject<RowCountValue> rowCountValue = new MutableObject<RowCountValue>();
        new StatementTemplate(connection).execute(
                new StatementFactory<Statement>() {
                    @Override
                    public Statement create(Connection connection) throws SQLException {
                        return connection.createStatement();
                    }
                }, new StatementCallback<Statement>() {
                    @Override
                    public void execute(Statement statement) throws SQLException {
                        rowCountValue.setValue(executeRowCountQuery(statement, rowCountQuery));
                    }
                }
        );
        return rowCountValue.getValue();
    }

    protected RowCountValue executeRowCountQuery(Statement statement, RowCountQuery rowCountQuery) throws SQLException {
        ResultSet rowCount = statement.executeQuery(rowCountQuery.getQuery().toQuery());
        RowCountValue rowCountValue = null;
        if (rowCount.next()) {
            rowCountValue = extractRowCountValue(rowCount, rowCountQuery);
        }
        return rowCountValue;
    }

    protected RowCountValue extractRowCountValue(ResultSet rowCount, RowCountQuery rowCountQuery) throws SQLException {
        return new RowCountValue(rowCountQuery, rowCount.getLong(1));
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
        clause = getScriptQuoted(clause);
        if (!clause.startsWith("(") && !clause.endsWith(")")) {
            return "(" + clause + ")";
        } else {
            return clause;
        }
    }

    @Override
    public String getDefaultValue(int typeCode, String defaultValue, Dialect dialect) {
        if (defaultValue == null) {
            return null;
        }
        defaultValue = getScriptTranslation(dialect.getDatabaseInfo(), defaultValue);
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

    protected String getScriptQuoted(String script) {
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

    @Override
    public String getJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeDesc jdbcTypeDesc,
                                  JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        String jdbcTypeName = getJdbcTypeName(getJdbcTypeNameMap(databaseInfo), jdbcTypeDesc,
                jdbcTypeSpecifiers);
        if (jdbcTypeName == null) {
            jdbcTypeName = getJdbcTypeName(getJdbcTypeNameMap(), jdbcTypeDesc, jdbcTypeSpecifiers);
        }
        return jdbcTypeName;
    }

    protected void addJdbcType(JdbcType type) {
        getJdbcTypeRegistry().addJdbcType(type);
    }

    protected void addJdbcTypeAdapter(JdbcTypeAdapter typeAdapter) {
        getJdbcTypeRegistry().addJdbcTypeAdapter(typeAdapter);
    }

    protected void addJdbcTypeDescAlias(int typeCode, String typeName, int typeCodeAlias) {
        getJdbcTypeRegistry().addJdbcTypeDescAlias(typeCode, typeName, typeCodeAlias);
    }

    protected void addJdbcTypeName(int typeCode, String typeName) {
        getJdbcTypeNameMap().addJdbcTypeName(typeCode, typeName);
    }

    protected void addJdbcTypeName(int typeCode, String typeName, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        getJdbcTypeNameMap().addJdbcTypeName(typeCode, typeName, jdbcTypeSpecifiers);
    }

    protected void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, String typeName) {
        getJdbcTypeNameMap().addJdbcTypeName(jdbcTypeDesc, typeName);
    }

    protected void addJdbcTypeName(JdbcTypeDesc jdbcTypeDesc, String typeName, JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        getJdbcTypeNameMap().addJdbcTypeName(jdbcTypeDesc, typeName, jdbcTypeSpecifiers);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, int typeCode, String typeName) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(typeCode, typeName);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, int typeCode, String typeName,
                                   JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(typeCode, typeName, jdbcTypeSpecifiers);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeDesc jdbcTypeDesc, String typeName) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(jdbcTypeDesc, typeName);
    }

    protected void addJdbcTypeName(DatabaseInfo databaseInfo, JdbcTypeDesc jdbcTypeDesc, String typeName,
                                   JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        getJdbcTypeNameMap(databaseInfo).addJdbcTypeName(jdbcTypeDesc, typeName, jdbcTypeSpecifiers);
    }

    protected String getJdbcTypeName(JdbcTypeNameMap jdbcTypeNameMap, JdbcTypeDesc jdbcTypeDesc,
                                     JdbcTypeSpecifiers jdbcTypeSpecifiers) {
        String jdbcTypeName = jdbcTypeNameMap.getJdbcTypeName(jdbcTypeDesc, jdbcTypeSpecifiers);
        if (jdbcTypeName == null) {
            jdbcTypeName = jdbcTypeNameMap.getJdbcTypeName(
                    new JdbcTypeDesc(jdbcTypeDesc.getTypeCode()), jdbcTypeSpecifiers);
        }
        return jdbcTypeName;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleDialect that = (SimpleDialect) o;

        if (identifierNormalizer != null ? !identifierNormalizer.equals(
                that.identifierNormalizer) : that.identifierNormalizer != null) return false;
        if (jdbcTypeNameMaps != null ? !jdbcTypeNameMaps.equals(that.jdbcTypeNameMaps) : that.jdbcTypeNameMaps != null)
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
