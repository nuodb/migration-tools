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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.*;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class DatabaseInfoTest {

    @DataProvider(name = "isAssignable")
    public Object[][] createIsAssignableData() throws Exception {
        return new Object[][] { { new DatabaseInfo(), new DatabaseInfo(), true },
                { new DatabaseInfo(), new DatabaseInfo("NuoDB"), true },
                { new DatabaseInfo("NuoDB"), new DatabaseInfo(), false },
                { new DatabaseInfo("NuoDB"), new DatabaseInfo("PostgreSQL"), false },
                { new DatabaseInfo("NuoDB"), new DatabaseInfo("NuoDB", "2.0.1"), true },
                { new DatabaseInfo("NuoDB", "2.0.1"), new DatabaseInfo("NuoDB", "2.0.1"), true },
                { new DatabaseInfo("NuoDB", "2.0.2"), new DatabaseInfo("NuoDB", "2.0.1"), false },
                { new DatabaseInfo("NuoDB", "2.0.2"), new DatabaseInfo("NuoDB", "2.0.1"), false },
                { new DatabaseInfo("NuoDB"), new DatabaseInfo("NuoDB", "2.0.1", 1), true },
                { new DatabaseInfo("NuoDB"), new DatabaseInfo("NuoDB", "2.0.1", 1, 27), true },
                { new DatabaseInfo("NuoDB", null, 1), new DatabaseInfo("NuoDB", "2.0.1", 2), true },
                { new DatabaseInfo("NuoDB", null, 2), new DatabaseInfo("NuoDB", "2.0.1", 1), false },
                { new DatabaseInfo("NuoDB", null, 1), new DatabaseInfo("NuoDB", "2.0.1", 2, 1), true },
                { new DatabaseInfo("NuoDB", null, 1), new DatabaseInfo("NuoDB", "2.0.1", 1, 27), true },
                { new DatabaseInfo("NuoDB", null, 1, 26), new DatabaseInfo("NuoDB", "2.0.1", 1, 27), true },
                { new DatabaseInfo("NuoDB", null, 1, 28), new DatabaseInfo("NuoDB", "2.0.1", 1, 27), false },

                { new DatabaseInfo(null, null, null, null), new DatabaseInfo(null, null, null, 27), true },
                { new DatabaseInfo(null, null, null, 25), new DatabaseInfo(null, null, null, 27), true },
                { new DatabaseInfo(null, null, null, 27), new DatabaseInfo(null, null, null, 25), false },
                { new DatabaseInfo(null, null, null, 25), new DatabaseInfo(null, null, 1, 27), true },
                { new DatabaseInfo(null, null, 1, 20), new DatabaseInfo(null, null, 1, 27), true },
                { new DatabaseInfo(null, null, 1, 30), new DatabaseInfo(null, null, 1, 27), false },

                { DB2, new DatabaseInfo("DB2/Darwin"), true },
                { DB2, new DatabaseInfo("NuoDB", "2.0.1", 1, 27), false }, { NUODB_BASE, NUODB_203, true },
                { NUODB_203, NUODB_206, true }, { NUODB_206, NUODB, true }, { MSSQL_SERVER, MSSQL_SERVER_2005, true },
                { ORACLE, POSTGRE_SQL, false }, { POSTGRE_SQL, ORACLE, false }, };
    }

    @Test(dataProvider = "isAssignable")
    public void testIsAssignable(DatabaseInfo databaseInfoBase, DatabaseInfo databaseInfo, boolean assignable) {
        assertEquals(databaseInfoBase.isAssignable(databaseInfo), assignable,
                format("%s.isAssignable(%s)", databaseInfoBase, databaseInfo));
    }
}
