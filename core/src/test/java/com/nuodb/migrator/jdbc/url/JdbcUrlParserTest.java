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
package com.nuodb.migrator.jdbc.url;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.nuodb.migrator.jdbc.url.JdbcUrlConstants.PROTOCOL;
import static com.nuodb.migrator.jdbc.url.MySQLJdbcUrl.DEFAULT_BEHAVIOR;
import static com.nuodb.migrator.jdbc.url.MySQLJdbcUrl.ZERO_DATE_TIME_BEHAVIOR;
import static org.testng.Assert.*;

/**
 * @author Sergey Bushik
 */
public class JdbcUrlParserTest {

    private static final Map<String, Object> EMPTY_PROPERTIES = new PropertiesBuilder().build();

    @DataProvider(name = "parseUrl")
    public Object[][] createParseUrlData() {
        return new Object[][] {
                { "jdbc:jtds:sqlserver://localhost:1433/test;autoCommit=true;batchSize=0", "jtds", "sqlserver", null,
                        null, new PropertiesBuilder("autoCommit", "true").put("batchSize", "0").build() },
                { "jdbc:sqlserver://localhost:1433;database=test", "sqlserver", null, null, null,
                        new PropertiesBuilder("database", "test").build() },
                { "jdbc:mysql://localhost:3306/database", "mysql", null, "database", null,
                        new PropertiesBuilder(ZERO_DATE_TIME_BEHAVIOR, DEFAULT_BEHAVIOR).build() },
                { "jdbc:mysql://localhost:3306/database?connectTimeout=1000", "mysql", null, "database", null,
                        new PropertiesBuilder(ZERO_DATE_TIME_BEHAVIOR, DEFAULT_BEHAVIOR).put("connectTimeout", "1000")
                                .build() },
                { "jdbc:com.nuodb://localhost/database", "com.nuodb", null, null, null, EMPTY_PROPERTIES },
                { "jdbc:com.nuodb://localhost/database?user=admin&schema=test", "com.nuodb", null, null, "test",
                        new PropertiesBuilder("user", "admin").put("schema", "test").build() },
                { "jdbc:oracle:thin:@//localhost:1521/sid", "oracle", "thin", null, null, EMPTY_PROPERTIES },
                { "jdbc:postgresql:localhost?user=admin&searchpath=schema", "postgresql", null, null, "schema",
                        new PropertiesBuilder("user", "admin").put("searchpath", "schema").build() } };
    }

    @Test(dataProvider = "parseUrl")
    public void testParseUrl(String url, String subProtocol, String qualifier, String catalog, String schema,
            Map<String, Object> properties) {
        JdbcUrlParser jdbcUrlParser = JdbcUrlParsers.getInstance().getParser(url);

        assertNotNull(jdbcUrlParser);
        assertTrue(jdbcUrlParser.canParse(url));

        JdbcUrl jdbcUrl = jdbcUrlParser.parse(url, null);

        assertNotNull(jdbcUrl);
        assertEquals(jdbcUrl.getProtocol(), PROTOCOL);
        assertEquals(jdbcUrl.getSubProtocol(), subProtocol);
        assertEquals(jdbcUrl.getQualifier(), qualifier);
        assertEquals(jdbcUrl.getCatalog(), catalog);
        assertEquals(jdbcUrl.getSchema(), schema);
        assertEquals(jdbcUrl.getParameters(), properties);
    }

    @DataProvider(name = "userPasswordData")
    public Object[][] createUserPasswordData() {
        return new Object[][] {
                { "jdbc:jtds:sqlserver://localhost:1433/test;user=admin;password=root", "jtds",
                        new PropertiesBuilder("user", "admin").put("password", "root").build() },
                { "jdbc:sqlserver://localhost:1433;user=admin;password=root", "sqlserver",
                        new PropertiesBuilder("user", "admin").put("password", "root").build() },
                { "jdbc:mysql://localhost:3306/database?user=admin&password=root", "mysql",
                        new PropertiesBuilder("user", "admin").put("password", "root")
                                .put(ZERO_DATE_TIME_BEHAVIOR, DEFAULT_BEHAVIOR).build() },
                { "jdbc:postgresql:localhost?user=admin&&password=root", "postgresql",
                        new PropertiesBuilder("user", "admin").put("password", "root").build() },
                { "jdbc:oracle:thin:scott/tiger@//localhost:1521/sid", "oracle",
                        new PropertiesBuilder("user", "scott").put("password", "tiger").build() },
                { "jdbc:oracle:thin:scott@//localhost:1521/sid", "oracle",
                        new PropertiesBuilder("user", "scott").build() },
                { "jdbc:oracle:thin:@//localhost:1521/sid", "oracle", EMPTY_PROPERTIES },
                { "jdbc:com.nuodb://localhost/database?schema=test&user=admin&password=root", "com.nuodb",
                        new PropertiesBuilder("user", "admin").put("password", "root").put("schema", "test").build() },
                { "jdbc:db2://localhost:5000/database:user=admin;password=root;", "db2",
                        new PropertiesBuilder("user", "admin").put("password", "root").build() },
                { "jdbc:db2://localhost:5000/database", "db2", EMPTY_PROPERTIES }
                // jdbc:db2://127.0.0.1:50000/SAMPLE:user=root;password=root;
        };
    }

    @Test(dataProvider = "userPasswordData")
    public void testUserPasswordParseUrl(String url, String subProtocol, Map<String, Object> properties) {
        JdbcUrlParser jdbcUrlParser = JdbcUrlParsers.getInstance().getParser(url);

        assertNotNull(jdbcUrlParser);
        assertTrue(jdbcUrlParser.canParse(url));

        JdbcUrl jdbcUrl = jdbcUrlParser.parse(url, null);

        assertNotNull(jdbcUrl);
        assertEquals(jdbcUrl.getParameters(), properties);
    }

    static class PropertiesBuilder {

        private Map<String, Object> properties = new HashMap<String, Object>();

        public PropertiesBuilder() {
        }

        public PropertiesBuilder(String key, Object value) {
            put(key, value);
        }

        public PropertiesBuilder put(String key, Object value) {
            properties.put(key, value);
            return this;
        }

        public Map<String, Object> build() {
            return properties;
        }
    }
}
