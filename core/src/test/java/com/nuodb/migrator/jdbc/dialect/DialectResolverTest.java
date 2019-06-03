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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * @author Sergey Bushik
 */
public class DialectResolverTest {

    private DialectResolver dialectResolver;

    @BeforeMethod
    public void init() {
        dialectResolver = new SimpleDialectResolver();
    }

    @Test
    public void testResolve() throws SQLException {
        assertTrue(dialectResolver.resolve(new DatabaseInfo("NuoDB", "1.0.0-118", 15, 1)) instanceof NuoDBDialect);
        assertTrue(dialectResolver.resolve(new DatabaseInfo("MySQL", "5.5.5.28", 5, 5)) instanceof MySQLDialect);
        assertTrue(dialectResolver.resolve(new DatabaseInfo("PostgreSQL", "9.2.3", 2, 9)) instanceof PostgreSQLDialect);
        assertTrue(dialectResolver.resolve(new DatabaseInfo("Oracle",
                "Oracle Database 11g Enterprise Edition Release 11.2.0.1.0 - 64bit Production\n"
                        + "With the Partitioning, OLAP, Data Mining and Real Application Testing options",
                2, 11)) instanceof OracleDialect);
        assertTrue(dialectResolver
                .resolve(new DatabaseInfo("Microsoft SQL Server", "8", 0, 8)) instanceof MSSQLServerDialect);
        assertTrue(dialectResolver.resolve(
                new DatabaseInfo("Microsoft SQL Server", "11.00.3339", 0, 11)) instanceof MSSQLServer2005Dialect);
        assertNotNull(dialectResolver.resolve(new DatabaseInfo("DB2", "DSN10015", 10, 1)));
    }
}
