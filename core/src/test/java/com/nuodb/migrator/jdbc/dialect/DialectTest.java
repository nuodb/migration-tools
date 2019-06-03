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

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.type.JdbcEnumType;
import com.nuodb.migrator.jdbc.type.JdbcSetType;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.dialect.TranslatorUtils.createScript;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;
import static java.lang.String.format;
import static java.sql.Types.*;
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
    public Object[][] createZeroDateTimeBehaviorData() throws Exception {
        Session convertToNull = createSession(new MySQLDialect(MYSQL),
                "jdbc:mysql://localhost:3306/test?zeroDateTimeBehavior=convertToNull");
        Session round = createSession(new MySQLDialect(MYSQL),
                "jdbc:mysql://localhost:3306/test?zeroDateTimeBehavior=round");
        return new Object[][] { { createScript("00:00:00", Types.TIME, "TIME"), convertToNull, "'00:00:00'" },
                { createScript("0000-00-00", DATE, "DATE"), convertToNull, "'0000-00-00'" },
                { createScript("0000-00-00 00:00:00", Types.TIMESTAMP, "DATE"), convertToNull,
                        "'0000-00-00 00:00:00'" },
                { createScript("00:00:00", Types.TIME, "TIME"), round, "00:00:00" },
                { createScript("0000-00-00", DATE, "DATE"), round, "0001-01-01" },
                { createScript("0000-00-00 00:00:00", Types.TIMESTAMP, "DATE"), round, "0001-01-01 00:00:00" }, };
    }

    @DataProvider(name = "zeroDateTimeBehaviorException")
    public Object[][] createZeroDateTimeBehaviorExceptionData() throws Exception {
        Session session = createSession(new MySQLDialect(MYSQL),
                "jdbc:mysql://localhost:3306/test?zeroDateTimeBehavior=exception");
        return new Object[][] { { createScript("00:00:00", Types.TIME, "TIME"), session },
                { createScript("0000-00-00", DATE, "DATE"), session },
                { createScript("0000-00-00 00:00:00", Types.TIMESTAMP, "DATE"), session } };
    }

    @Test(dataProvider = "zeroDateTimeBehavior")
    public void testZeroDateTimeBehavior(Script script, Session session, String translation) {
        assertEquals(dialect.translate(script, session).getScript(), translation);
    }

    @Test(dataProvider = "zeroDateTimeBehaviorException", expectedExceptions = TranslatorException.class)
    public void testZeroDateTimeBehaviorException(Script script, Session session) {
        dialect.translate(script, session);
        fail("Should fail for zeroDateTimeBehavior=exception");
    }

    @DataProvider(name = "getCheckClause")
    public Object[][] createGetCheckClauseData() {
        return new Object[][] { { null, null }, { "F1 > 5", "(F1 > 5)" }, { "F1 in (0,1)", "(F1 in (0,1))" },
                { "(F1 < 1) AND (F2 < 2)", "((F1 < 1) AND (F2 < 2))" }, };
    }

    @Test(dataProvider = "getCheckClause")
    public void testGetCheckClause(String sourceCheckClause, String checkClause) {
        assertEquals(dialect.getCheckClause(sourceCheckClause), checkClause);
    }

    @DataProvider(name = "getDefaultValue")
    public Object[][] createGetDefaultValueData() throws Exception {
        Session session = createSession(new MySQLDialect(MYSQL),
                "jdbc:mysql://localhost:3306/test?zeroDateTimeBehavior=convertToNull");
        Column date = new Column();
        date.setJdbcType(new JdbcType(new JdbcTypeDesc(DATE, "date")));
        date.setDefaultValue(valueOf("0000-00-00"));
        Column time = new Column();
        time.setJdbcType(new JdbcType(new JdbcTypeDesc(TIME, "time")));
        time.setDefaultValue(valueOf("00:00:00"));
        Column timestamp = new Column();
        timestamp.setJdbcType(new JdbcType(new JdbcTypeDesc(TIMESTAMP, "timestamp")));
        timestamp.setDefaultValue(valueOf("0000-00-00 00:00:00"));
        Column timestamp1 = new Column();
        timestamp1.setJdbcType(new JdbcType(new JdbcTypeDesc(TIMESTAMP, "timestamp")));
        timestamp1.setDefaultValue(null);
        timestamp1.setNullable(true);
        Column timestamp2 = new Column();
        timestamp2.setJdbcType(new JdbcType(new JdbcTypeDesc(TIMESTAMP, "timestamp")));
        timestamp2.setDefaultValue(null);
        timestamp2.setNullable(false);
        Column timestamp3 = new Column();
        timestamp3.setJdbcType(new JdbcType(new JdbcTypeDesc(TIMESTAMP, "timestamp")));
        timestamp3.setDefaultValue(valueOf("0000-00-00 00:00:00"));
        timestamp3.setNullable(true);
        Column varchar = new Column();
        varchar.setJdbcType(new JdbcType(new JdbcTypeDesc(VARCHAR, "varchar")));
        varchar.setDefaultValue(valueOf("NULL"));
        return new Object[][] { { session, date, "'0000-00-00'" }, { session, time, "'00:00:00'" },
                { session, timestamp, "'0000-00-00 00:00:00'" }, { session, timestamp1, null },
                { session, timestamp2, null }, { session, timestamp3, "NULL" }, { session, varchar, "NULL" } };
    }

    @Test(dataProvider = "getDefaultValue")
    public void testGetDefaultValue(Session session, Column column, String defaultValue) {
        assertEquals(dialect.getDefaultValue(column, session), defaultValue);
    }

    @DataProvider(name = "getTypeName")
    public Object[][] createGetTypeNameData() throws Exception {
        Collection<Object[]> data = newArrayList();
        createGetMySQLTypeNamesData(data);
        return data.toArray(new Object[data.size()][]);
    }

    private void createGetMySQLTypeNamesData(Collection<Object[]> data) {
        MySQLDialect dialect = new MySQLDialect(MYSQL);
        // #1 test INT(10)
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(4, "INT"), newOptions(10, 10, 0)),
                "INT(10)" });
        // #2 test DECIMAL(10, 2)
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(3, "DECIMAL"), newOptions(10, 10, 2)),
                "DECIMAL(10,2)" });
        // #3 test DATETIME
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(93, "DATETIME")), "DATETIME" });
        // #4 test TIMESTAMP
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(93, "TIMESTAMP")), "TIMESTAMP" });
        // #6 test YEAR(4)
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(91, "YEAR"), newOptions(0, 0, 0)),
                "YEAR" });
        // #7 test CHAR(20)
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(1, "CHAR"), newOptions(20, 20, 0)),
                "CHAR(20)" });
        // #8 test ENUM('abcd','check','sample test')
        data.add(new Object[] { dialect, MYSQL, new JdbcEnumType(new JdbcType(new JdbcTypeDesc(1, "ENUM")),
                newArrayList("abcd", "check", "sample test")), "ENUM('abcd','check','sample test')" });
        // #9 test SET('one','two','','three')
        data.add(new Object[] { dialect, MYSQL,
                new JdbcSetType(new JdbcType(new JdbcTypeDesc(1, "SET")), newArrayList("one", "two", "", "three")),
                "SET('one','two','','three')" });
        // #10 test TINYINT
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(-6, "TINYINT")), "TINYINT" });
        // #11 test VARCHAR(20)
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(12, "VARCHAR"), newOptions(20, 20, 0)),
                "VARCHAR(20)" });
        // #12 test TINYINT
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(-6, "TINYINT"), newOptions(6, 6, 0)),
                "TINYINT" });
        // #13 test TEXT
        data.add(new Object[] { dialect, MYSQL,
                new JdbcType(new JdbcTypeDesc(2005, "TEXT"), newOptions(65535, 65535, 0)), "TEXT" });
        // #14 test DATE
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(91, "DATE"), newOptions(10, 10, 0)),
                "DATE" });
        // #15 test SMALLINT
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(5, "SMALLINT")), "SMALLINT" });
        // #16 test TINYBLOB
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(-2, "TINYBLOB"), newOptions(255, 255, 0)),
                "TINYBLOB" });
        // #17 test BIGINT UNSIGNED
        data.add(new Object[] { dialect, MYSQL,
                new JdbcType(new JdbcTypeDesc(-5, "BIGINT UNSIGNED"), newOptions(20, 20, 0)), "BIGINT(20) UNSIGNED" });
        // #18 test TINYTEXT
        data.add(new Object[] { dialect, MYSQL, new JdbcType(new JdbcTypeDesc(12, "TINYTEXT"), newOptions(255, 255, 0)),
                "TINYTEXT" });
        // #19 test BLOB
        data.add(new Object[] { dialect, MYSQL,
                new JdbcType(new JdbcTypeDesc(2004, "BLOB"), newOptions(65535, 65535, 0)), "BLOB" });
        // #20 test MEDIUMBLOB
        data.add(new Object[] { dialect, MYSQL,
                new JdbcType(new JdbcTypeDesc(2004, "MEDIUMBLOB"), newOptions(16777215, 16777215, 0)), "MEDIUMBLOB" });
        // #20 test MEDIUMTEXT
        data.add(new Object[] { dialect, MYSQL,
                new JdbcType(new JdbcTypeDesc(2005, "MEDIUMTEXT"), newOptions(16777215, 16777215, 0)), "MEDIUMTEXT" });
        // #20 test LONGBLOB
        data.add(new Object[] { dialect, MYSQL,
                new JdbcType(new JdbcTypeDesc(-4, "LONGBLOB"), newOptions(2147483647, 2147483647, 0)), "LONGBLOB" });
        // #20 test LONGTEXT
        data.add(new Object[] { dialect, MYSQL,
                new JdbcType(new JdbcTypeDesc(-1, "LONGTEXT"), newOptions(2147483647, 2147483647, 0)), "LONGTEXT" });
    }

    @Test(dataProvider = "getTypeName")
    public void testGetTypeName(Dialect dialect, DatabaseInfo databaseInfo, JdbcType jdbcType, String typeName) {
        assertEquals(dialect.getTypeName(databaseInfo, jdbcType), typeName,
                format("Expecting %s type name for %s jdbc type", typeName, jdbcType));
    }
}
