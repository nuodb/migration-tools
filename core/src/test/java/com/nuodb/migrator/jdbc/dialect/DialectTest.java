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

import com.nuodb.migrator.jdbc.session.Session;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Types;

import static com.nuodb.migrator.jdbc.dialect.TranslatorUtils.createScript;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * @author Sergey Bushik
 */
public class DialectTest {

    private Dialect dialect;

    @BeforeClass
    public void init() {
        dialect = new NuoDBDialect();
    }

    @DataProvider(name = "zeroDateTimeBehavior")
    public Object[][] createZeroDateTimeBehaviorData() throws SQLException {
        Session convertToNull = createSession(new MySQLDialect(MYSQL),
                "jdbc:mysql://localhost:3306/test?zeroDateTimeBehavior=convertToNull");
        Session round = createSession(new MySQLDialect(MYSQL),
                "jdbc:mysql://localhost:3306/test?zeroDateTimeBehavior=round");
        return new Object[][]{
                {createScript("00:00:00", Types.TIME, "TIME", convertToNull), "NULL"},
                {createScript("0000-00-00", Types.DATE, "DATE", convertToNull), "NULL"},
                {createScript("0000-00-00 00:00:00", Types.TIMESTAMP, "DATE", convertToNull), "NULL"},
                {createScript("00:00:00", Types.TIME, "TIME", round), "00:00:00"},
                {createScript("0000-00-00", Types.DATE, "DATE", round), "0001-01-01"},
                {createScript("0000-00-00 00:00:00", Types.TIMESTAMP, "DATE", round), "0001-01-01 00:00:00"},
        };
    }

    @DataProvider(name = "zeroDateTimeBehaviorException")
    public Object[][] createZeroDateTimeBehaviorExceptionData() throws SQLException {
        Session exception = createSession(new MySQLDialect(MYSQL),
                "jdbc:mysql://localhost:3306/test?zeroDateTimeBehavior=exception");
        return new Object[][]{
                {createScript("00:00:00", Types.TIME, "TIME", exception)},
                {createScript("0000-00-00", Types.DATE, "DATE", exception)},
                {createScript("0000-00-00 00:00:00", Types.TIMESTAMP, "DATE", exception)}
        };
    }

    @Test(dataProvider = "zeroDateTimeBehavior")
    public void testZeroDateTimeBehavior(Script script, String translation) {
        assertEquals(dialect.translate(script).getScript(), translation);
    }

    @Test(dataProvider = "zeroDateTimeBehaviorException", expectedExceptions = TranslatorException.class)
    public void testZeroDateTimeBehaviorException(Script script) {
        dialect.translate(script);
        fail("Should fail for zeroDateTimeBehavior=exception");
    }

    @DataProvider(name = "getCheckClause")
    public Object[][] createGetCheckClauseData() {
        return new Object[][]{
                {null, null},
                {"F1 > 5", "(F1 > 5)"},
                {"F1 in (0,1)", "(F1 in (0,1))"},
        };
    }

    @Test(dataProvider = "getCheckClause")
    public void testGetCheckClause(String sourceCheckClause, String checkClause) {
        assertEquals(dialect.getCheckClause(sourceCheckClause), checkClause);
    }
}
