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
package com.nuodb.hibernate.dialect;

import org.hibernate.MappingException;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.SQLFunctionTemplate;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.function.VarArgsSQLFunction;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class NuoDBDialect extends Dialect {

    public NuoDBDialect() {
        registerTypes();
        registerFunctions();
        getDefaultProperties().setProperty(Environment.MAX_FETCH_DEPTH, "2");
        getDefaultProperties().setProperty(Environment.STATEMENT_BATCH_SIZE, DEFAULT_BATCH_SIZE);
    }

    private void registerTypes() {
        registerColumnType(Types.BIT, "SMALLINT");
        registerColumnType(Types.TINYINT, "SMALLINT");
        registerColumnType(Types.SMALLINT, "SMALLINT");
        registerColumnType(Types.INTEGER, "INTEGER");
        registerColumnType(Types.BIGINT, "BIGINT");
        registerColumnType(Types.FLOAT, "FLOAT");
        registerColumnType(Types.REAL, "REAL");
        registerColumnType(Types.DOUBLE, "DOUBLE");
        registerColumnType(Types.NUMERIC, "NUMERIC($p, $s)");
        registerColumnType(Types.DECIMAL, "DECIMAL($p, $s)");
        registerColumnType(Types.CHAR, "CHARACTER");
        registerColumnType(Types.VARCHAR, "CHARACTER VARYING ($l)");
        registerColumnType(Types.LONGVARCHAR, "CHARACTER VARYING ($l)");
        registerColumnType(Types.DATE, "DATE");
        registerColumnType(Types.TIME, "TIMEONLY");
        registerColumnType(Types.TIMESTAMP, "TIMESTAMP");
        registerColumnType(Types.BINARY, "BINARY");
        registerColumnType(Types.VARBINARY, "BINARY VARYING ($l)");
        registerColumnType(Types.LONGVARBINARY, "BINARY VARYING ($l)");
        registerColumnType(Types.NULL, "NULL");
        // TODO registerColumnType(Types.OTHER,"?");
        // TODO registerColumnType(Types.JAVA_OBJECT,"?");
        // TODO registerColumnType(Types.DISTINCT,"?");
        // TODO registerColumnType(Types.STRUCT,"?");
        // TODO registerColumnType(Types.ARRAY,"?");
        registerColumnType(Types.BLOB, "BINARY LARGE OBJECT");
        registerColumnType(Types.CLOB, "CHARACTER LARGE OBJECT");
        // TODO registerColumnType(Types.REF,"?");
        // TODO registerColumnType(Types.DATALINK,"?");
        registerColumnType(Types.BOOLEAN, "BOOLEAN");
        // TODO registerColumnType(Types.ROWID,"?");
        registerColumnType(Types.NCHAR, "NATIONAL CHARACTER");
        registerColumnType(Types.NVARCHAR, "NATIONAL CHARACTER VARYING");
        // TODO registerColumnType(Types.LONGNVARCHAR,"?");
        registerColumnType(Types.NCLOB, "NATIONAL CHARACTER LARGE OBJECT");
        // TODO registerColumnType(Types.SQLXML,"?");
    }

    private void registerFunctions() {
        // TODO what other functions?
        registerFunction("concat", new VarArgsSQLFunction(StandardBasicTypes.STRING, "", "||", ""));
        registerFunction("length", new StandardSQLFunction("char_length", StandardBasicTypes.LONG));
        registerFunction("substring", new SQLFunctionTemplate(StandardBasicTypes.STRING, "substr(?1, ?2, ?3)"));
    }

    /**
     * Register a function that takes arguments.properties and has a variable return type.
     */
    private void registerFunction(String name) {
        registerFunction(name, new StandardSQLFunction(name));
    }

    /**
     * Register a function that takes arguments.properties and has a static return type.
     */
    private void registerFunction(String name, Type returnType) {
        registerFunction(name, new StandardSQLFunction(name, returnType));
    }

    /**
     * Register a function that does not take arguments.properties and has a static return type.
     */
    private void registerNoArgFunction(String name, Type returnType) {
        registerFunction(name, new NoArgSQLFunction(name, returnType));
    }

    @Override
    public String getAddColumnString() {
        return "add column";
    }

    @Override
    public boolean qualifyIndexName() {
        return false;
    }

    @Override
    public boolean supportsIdentityColumns() {
        return true;
    }

    @Override
    protected String getIdentityColumnString() throws MappingException {
        // TODO does "not null" belong here? Its necessary to force the primary key
        // to be not null.
        return "generated by default not null";
    }

    private StringBuilder join(StringBuilder buf, String separator, String[] values) {
        boolean separate = false;

        for (String value : values) {
            if (separate) {
                buf.append(separator);
            }
            buf.append(value);
            separate = true;
        }

        return buf;
    }

    @Override
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean supportsVariableLimit() {
        return false; // we can't define offset or limit params
    }

    @Override
    public String getLimitString(String sql, int offset, int limit) {
        StringBuilder sb = new StringBuilder(sql.length() + 20);
        sb.append(sql);
        sb.append(" offset ");
        sb.append(offset);
        sb.append(" fetch ");
        sb.append(limit);

        return sb.toString();
    }

    @Override
    public boolean dropConstraints() {
        return false; // temporary because "drop constraint" is never happy.
    }

    @Override
    public String getDropForeignKeyString() {
        return " drop constraint foreign ";
    }

    @Override
    public boolean supportsIfExistsAfterTableName() {
        return true;
    }

    @Override
    public boolean supportsIfExistsBeforeTableName() {
        return false;
    }

    @Override
    public String getSelectGUIDString() {
        return "select uuid()";
    }

    @Override
    public boolean supportsCascadeDelete() {
        return false;
    }

    @Override
    public boolean supportsTemporaryTables() {
        return false;
    }

    @Override
    public String getCrossJoinSeparator() {
        return ", ";
    }

    @Override
    public Boolean performTemporaryTableDDLInIsolation() {
        // because we [drop *temporary* table...] we do not
        // have to doAfterTransactionCompletion these in isolation.
        return Boolean.FALSE;
    }

    @Override
    public String getCastTypeName(int code) {
        if (code == Types.INTEGER) {
            return "signed";
        } else if (code == Types.VARCHAR) {
            return "char";
        } else if (code == Types.VARBINARY) {
            return "binary";
        } else {
            return super.getCastTypeName(code);
        }
    }

    @Override
    public int registerResultSetOutParameter(CallableStatement statement, int col)
            throws SQLException {
        return col;
    }

    @Override
    public ResultSet getResultSet(CallableStatement ps) throws SQLException {
        boolean isResultSet = ps.execute();
        while (!isResultSet && ps.getUpdateCount() != -1) {
            isResultSet = ps.getMoreResults();
        }
        return ps.getResultSet();
    }

    @Override
    public boolean supportsRowValueConstructorSyntax() {
        return true;
    }

    // locking support

    @Override
    public String getForUpdateString() {
        return " for update";
    }

    @Override
    public String getWriteLockString(int timeout) {
        return " for update";
    }

    @Override
    public String getReadLockString(int timeout) {
        return " lock in share mode";
    }

    @Override
    public boolean supportsEmptyInList() {
        return false;
    }

    @Override
    public boolean areStringComparisonsCaseInsensitive() {
        return true;
    }

    @Override
    public boolean supportsLobValueChangePropogation() {
        // note: at least my local MySQL 5.1 install shows this not working...
        return false;
    }

    @Override
    public boolean supportsSubqueryOnMutatingTable() {
        return false;
    }
}