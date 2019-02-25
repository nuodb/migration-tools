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
import com.nuodb.migrator.jdbc.metadata.resolver.SimpleCachingServiceResolver;

import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.*;
import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;

/**
 * @author Sergey Bushik
 */
public class SimpleDialectResolver extends SimpleCachingServiceResolver<Dialect> implements DialectResolver {

    public SimpleDialectResolver() {
        super(SimpleDialect.class);
        register(DB2, DB2Dialect.class);
        register(MYSQL, MySQLDialect.class);
        register(NUODB_BASE, NuoDBDialect.class);
        register(NUODB_203, NuoDBDialect203.class);
        register(NUODB_206, NuoDBDialect206.class);
        register(NUODB_256, NuoDBDialect256.class);
        register(NUODB_320, NuoDBDialect320.class);
        register(NUODB, NuoDBDialect340.class);
        register(POSTGRE_SQL, PostgreSQLDialect.class);
        register(ORACLE, OracleDialect.class);
        register(MSSQL_SERVER, MSSQLServerDialect.class);
        register(MSSQL_SERVER_2005, MSSQLServer2005Dialect.class);
    }

    @Override
    protected Dialect createService(Class<? extends Dialect> serviceClass, DatabaseInfo databaseInfo) {
        return newInstance(serviceClass, databaseInfo);
    }
}
