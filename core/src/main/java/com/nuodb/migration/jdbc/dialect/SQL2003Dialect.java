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

import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.metadata.ReferenceAction;
import com.nuodb.migration.jdbc.type.*;
import com.nuodb.migration.jdbc.type.jdbc4.Jdbc4TypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static java.sql.Connection.*;
import static org.apache.commons.lang3.StringUtils.endsWith;
import static org.apache.commons.lang3.StringUtils.startsWith;

/**
 * @author Sergey Bushik
 */
public class SQL2003Dialect implements Dialect {

    private static final Pattern IDENTIFIER = Pattern.compile("[a-zA-Z0-9_]*");
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private JdbcTypeRegistry jdbcTypeRegistry;

    public SQL2003Dialect() {
        this(new Jdbc4TypeRegistry());
    }

    public SQL2003Dialect(JdbcTypeRegistry jdbcTypeRegistry) {
        this.jdbcTypeRegistry = jdbcTypeRegistry;

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
    public String getIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        if (isQuote(identifier)) {
            return quote(identifier);
        } else {
            return normalize(identifier);
        }
    }

    protected boolean isQuote(String identifier) {
        return !IDENTIFIER.matcher(identifier).matches();
    }

    protected String quote(String identifier) {
        return openQuote() + identifier + closeQuote();
    }

    protected String normalize(String identifier) {
        return identifier;
    }

    protected char openQuote() {
        return '"';
    }

    protected char closeQuote() {
        return '"';
    }

    @Override
    public String getNoColumnsInsertString() {
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
    public boolean supportsTransactionIsolationLevel(int transactionIsolationLevel) throws SQLException {
        return Lists.newArrayList(
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
    public JdbcTypeRegistry getJdbcTypeRegistry() {
        return jdbcTypeRegistry;
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
    public boolean supportsWithTimezone() {
        return true;
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
    public String getIdentityColumnString() {
        return "";
    }

    @Override
    public String getTableCheck(String check) {
        return getConstraint(check);
    }

    @Override
    public String getColumnCheck(String check) {
        return getConstraint(check);
    }

    @Override
    public String getDefaultValue(int typeCode, String defaultValue) {
        if (defaultValue == null) {
            return null;
        }
        StringBuilder buffer = new StringBuilder(defaultValue.length() + 2);
        if (!startsWith(defaultValue, "'")) {
            buffer.append("'");
        }
        buffer.append(defaultValue);
        if (!endsWith(defaultValue, "'")) {
            buffer.append("'");
        }
        return buffer.toString();
    }

    @Override
    public String getUpdateAction(ReferenceAction updateAction) {
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
    public String getCascadeConstraintsString() {
        return null;
    }

    @Override
    public String getDropForeignKeyString() {
        return "DROP CONSTRAINT";
    }

    @Override
    public String getDeleteAction(ReferenceAction deleteAction) {
        return null;
    }

    protected String getConstraint(String check) {
        StringBuilder buffer = new StringBuilder(check.length() + 2);
        if (!check.startsWith("(")) {
            buffer.append("(");
        }
        buffer.append(check);
        if (!check.endsWith(")")) {
            buffer.append(")");
        }
        return buffer.toString();
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
    public void stream(Statement statement) throws SQLException {
    }

    @Override
    public boolean dropConstraints() {
        return true;
    }

    public JdbcType getJdbcType(int typeCode) {
        return jdbcTypeRegistry.getJdbcType(typeCode);
    }

    public JdbcType getJdbcType(int typeCode, String typeName) {
        return jdbcTypeRegistry.getJdbcType(typeCode, typeName);
    }

    public JdbcType getJdbcType(JdbcTypeDesc typeDesc) {
        return jdbcTypeRegistry.getJdbcType(typeDesc);
    }

    public void addJdbcType(JdbcType jdbcType) {
        jdbcTypeRegistry.addJdbcType(jdbcType);
    }

    public void addJdbcTypes(Collection<JdbcType> jdbcTypes) {
        jdbcTypeRegistry.addJdbcTypes(jdbcTypes);
    }

    public void addJdbcTypes(JdbcTypeRegistry jdbcTypeRegistry) {
        this.jdbcTypeRegistry.addJdbcTypes(jdbcTypeRegistry);
    }

    public Collection<JdbcType> getJdbcTypes() {
        return jdbcTypeRegistry.getJdbcTypes();
    }

    public void addJdbcTypeAdapter(JdbcTypeAdapter jdbcTypeAdapter) {
        jdbcTypeRegistry.addJdbcTypeAdapter(jdbcTypeAdapter);
    }

    public void addJdbcTypeAdapters(Collection<JdbcTypeAdapter> jdbcTypeAdapters) {
        jdbcTypeRegistry.addJdbcTypeAdapters(jdbcTypeAdapters);
    }

    public Collection<JdbcTypeAdapter> getJdbcTypeAdapters() {
        return jdbcTypeRegistry.getJdbcTypeAdapters();
    }

    public JdbcTypeAdapter getJdbcTypeAdapter(Class typeClass) {
        return jdbcTypeRegistry.getJdbcTypeAdapter(typeClass);
    }

    public JdbcTypeNameMap getJdbcTypeNameMap() {
        return getJdbcTypeRegistry().getJdbcTypeNameMap();
    }

    public String getTypeName(int typeCode) {
        return getJdbcTypeNameMap().getTypeName(typeCode);
    }

    public String getTypeName(int typeCode, int size, int precision, int scale) {
        return getJdbcTypeNameMap().getTypeName(typeCode, size, precision, scale);
    }

    public void addTypeName(int typeCode, String typeName) {
        getJdbcTypeNameMap().addTypeName(typeCode, typeName);
    }

    public void addTypeName(int typeCode, String typeName, int size) {
        getJdbcTypeNameMap().addTypeName(typeCode, typeName, size);
    }
}
