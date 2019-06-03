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
package com.nuodb.migrator.jdbc.metadata;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Sergey Bushik
 */
public interface DatabaseInfos {
    final DatabaseInfo MYSQL = new DatabaseInfo("MySQL");
    final DatabaseInfo NUODB_BASE = new NuoDBDatabaseInfo("NuoDB");
    final DatabaseInfo NUODB_203 = new NuoDBDatabaseInfo("NuoDB", null, 2, 0, 29);
    final DatabaseInfo NUODB_204 = new NuoDBDatabaseInfo("NuoDB", null, 2, 0, 30);
    final DatabaseInfo NUODB_205 = new NuoDBDatabaseInfo("NuoDB", null, 2, 0, 31);
    final DatabaseInfo NUODB_206 = new NuoDBDatabaseInfo("NuoDB", null, 2, 0, 32);
    final DatabaseInfo NUODB_256 = new NuoDBDatabaseInfo("NuoDB", null, 2, 5, 33);
    final DatabaseInfo NUODB_320 = new NuoDBDatabaseInfo("NuoDB", null, 3, 2, 34);
    final DatabaseInfo NUODB = new NuoDBDatabaseInfo("NuoDB", null, 3, 4, 34);
    final DatabaseInfo ORACLE = new DatabaseInfo("Oracle");
    final DatabaseInfo DB2 = new DatabaseInfo("DB2/") {
        @Override
        protected ComparisonChain isAssignableProductName(DatabaseInfo databaseInfo, ComparisonChain comparator) {
            return comparator.compare(getProductName(), databaseInfo.getProductName(), new Ordering<String>() {
                @Override
                public int compare(String productName1, String productName2) {
                    return productName1 == null || StringUtils.startsWith(productName2, productName1) ? ASSIGNABLE
                            : NOT_ASSIGNABLE;
                }
            });
        }
    };
    final DatabaseInfo POSTGRE_SQL = new DatabaseInfo("PostgreSQL");
    final DatabaseInfo POSTGRE_SQL_83 = new DatabaseInfo("PostgreSQL", "8.3") {
        @Override
        protected ComparisonChain isAssignableProductVersion(DatabaseInfo databaseInfo, ComparisonChain comparator) {
            return comparator.compare(getProductVersion(), databaseInfo.getProductVersion(), new Ordering<String>() {
                @Override
                public int compare(String productVersion1, String productVersion2) {
                    return productVersion1 == null
                            || (productVersion2 != null && productVersion2.compareTo(productVersion1) >= 0) ? ASSIGNABLE
                                    : NOT_ASSIGNABLE;
                }
            });
        }
    };
    final DatabaseInfo MSSQL_SERVER = new DatabaseInfo("Microsoft SQL Server");
    final DatabaseInfo MSSQL_SERVER_2005 = new DatabaseInfo("Microsoft SQL Server", null, 0, 9);
}
