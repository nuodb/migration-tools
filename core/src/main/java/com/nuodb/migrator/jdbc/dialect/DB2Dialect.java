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

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.query.QueryLimit;

import java.sql.Types;

/**
 * @author Sergey Bushik
 */
public class DB2Dialect extends SimpleDialect {

    public DB2Dialect(DatabaseInfo databaseInfo) {
        super(databaseInfo);
    }

    @Override
    protected void initJdbcTypeNames() {
        addJdbcTypeName(Types.BIT, "SMALLINT");
        addJdbcTypeName(Types.BIGINT, "BIGINT");
        addJdbcTypeName(Types.SMALLINT, "SMALLINT");
        addJdbcTypeName(Types.TINYINT, "SMALLINT");
        addJdbcTypeName(Types.INTEGER, "INTEGER");
        addJdbcTypeName(Types.CHAR, "CHAR({N})");
        addJdbcTypeName(Types.VARCHAR, "VARCHAR({N})");
        addJdbcTypeName(Types.FLOAT, "FLOAT");
        addJdbcTypeName(Types.DOUBLE, "DOUBLE");
        addJdbcTypeName(Types.DATE, "DATE");
        addJdbcTypeName(Types.TIME, "TIME");
        addJdbcTypeName(Types.TIMESTAMP, "TIMESTAMP");
        addJdbcTypeName(Types.VARBINARY, "VARCHAR({N}) FOR BIT DATA");
        addJdbcTypeName(Types.NUMERIC, "NUMERIC({P},{S})");
        addJdbcTypeName(Types.BLOB, "BLOB({N})");
        addJdbcTypeName(Types.CLOB, "CLOB({N})");
        addJdbcTypeName(Types.LONGVARCHAR, "LONG VARCHAR");
        addJdbcTypeName(Types.LONGVARBINARY, "LONG VARCHAR FOR BIT DATA");
    }

    @Override
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean supportsCatalogs() {
        return true;
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }

    @Override
    public LimitHandler createLimitHandler(String query, QueryLimit queryLimit) {
        return new DB2LimitHandler(this, query, queryLimit);
    }
}
