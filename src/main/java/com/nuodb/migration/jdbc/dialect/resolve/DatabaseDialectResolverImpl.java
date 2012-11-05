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
package com.nuodb.migration.jdbc.dialect.resolve;

import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.dialect.*;
import com.nuodb.migration.utils.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class DatabaseDialectResolverImpl implements DatabaseDialectResolver {

    private final transient Log log = LogFactory.getLog(getClass());

    private Map<DatabaseInfoMatcher, Class<? extends DatabaseDialect>> databaseInfoMatchers = Maps.newHashMap();

    private Class<? extends DatabaseDialect> defaultDatabaseDialect = DatabaseDialectBase.class;

    public DatabaseDialectResolverImpl() {
        register("MySQL", MySQLDialect.class);
        register("PostgreSQL", PostgreSQLDialect.class);
        register("Microsoft SQL Server", SQLServerDialect.class);
        register("Oracle", OracleDialect.class);
        register("NuoDBDialect", NuoDBDialect.class);
    }

    public void register(String productName, Class<? extends DatabaseDialect> databaseDialectType) {
        register(new DatabaseInfoMatcherImpl(productName), databaseDialectType);
    }

    public void register(String productName, String productVersion,
                         Class<? extends DatabaseDialect> databaseDialectType) {
        register(new DatabaseInfoMatcherImpl(productName, productVersion), databaseDialectType);
    }

    public void register(String productName, String productVersion, int majorVersion,
                         Class<? extends DatabaseDialect> databaseDialectType) {
        register(new DatabaseInfoMatcherImpl(productName, productVersion, majorVersion), databaseDialectType);
    }

    public void register(String productName, String productVersion, int majorVersion, int minorVersion,
                         Class<? extends DatabaseDialect> databaseDialectType) {
        register(new DatabaseInfoMatcherImpl(productName, productVersion, majorVersion, minorVersion), databaseDialectType);
    }

    public void register(DatabaseInfoMatcher databaseInfoMatcher, Class<? extends DatabaseDialect> databaseDialectType) {
        databaseInfoMatchers.put(databaseInfoMatcher, databaseDialectType);
    }

    @Override
    public DatabaseDialect resolve(DatabaseMetaData metaData) throws SQLException {
        String productName = metaData.getDatabaseProductName();
        String productVersion = metaData.getDatabaseProductVersion();
        int minorVersion = metaData.getDatabaseMinorVersion();
        int majorVersion = metaData.getDatabaseMajorVersion();
        for (Map.Entry<DatabaseInfoMatcher, Class<? extends DatabaseDialect>> databaseInfoMatcherEntry : databaseInfoMatchers.entrySet()) {
            DatabaseInfoMatcher databaseInfoMatcher = databaseInfoMatcherEntry.getKey();
            if (databaseInfoMatcher.matches(productName, productVersion, minorVersion, majorVersion)) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Database dialect resolved %1$s", databaseInfoMatcherEntry.getValue().getName()));
                }
                return ClassUtils.newInstance(databaseInfoMatcherEntry.getValue());
            }
        }
        if (log.isWarnEnabled()) {
            log.warn(String.format("Database dialect defaulted to %1$s", defaultDatabaseDialect.getName()));
        }
        return ClassUtils.newInstance(defaultDatabaseDialect);
    }

    public Class<? extends DatabaseDialect> getDefaultDatabaseDialect() {
        return defaultDatabaseDialect;
    }

    public void setDefaultDatabaseDialect(Class<? extends DatabaseDialect> defaultDatabaseDialect) {
        this.defaultDatabaseDialect = defaultDatabaseDialect;
    }
}
