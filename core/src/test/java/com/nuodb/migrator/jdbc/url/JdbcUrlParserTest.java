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
package com.nuodb.migrator.jdbc.url;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * @author Sergey Bushik
 */
public class JdbcUrlParserTest {

    private static final Map<String, Object> EMPTY_PROPERTIES = new PropertiesBuilder().build();

    @DataProvider(name = "parseUrl")
    public Object[][] createParseUrlData() {
        return new Object[][]{
                {"jdbc:com.nuodb://localhost/database", null, null, EMPTY_PROPERTIES},
                {"jdbc:com.nuodb://localhost/database?user=admin&schema=test", null, "test",
                        new PropertiesBuilder().put("user", "admin").put("schema", "test").build()},
                {"jdbc:mysql://localhost:3306/database", "database", null, EMPTY_PROPERTIES},
                {"jdbc:mysql://localhost:3306/database?connectTimeout=1000", "database", null,
                        new PropertiesBuilder().put("connectTimeout", "1000").build()}
        };
    }

    @Test(dataProvider = "parseUrl")
    public void testParseUrl(String url, String catalog, String schema, Map<String, Object> properties) {
        JdbcUrlParser jdbcUrlParser = JdbcUrlParserUtils.getInstance().getParser(url);

        assertNotNull(jdbcUrlParser);
        assertTrue(jdbcUrlParser.canParse(url));

        JdbcUrl jdbcUrl = jdbcUrlParser.parse(url, null);

        assertNotNull(jdbcUrl);
        assertEquals(jdbcUrl.getCatalog(), catalog);
        assertEquals(jdbcUrl.getSchema(), schema);
        assertEquals(jdbcUrl.getProperties(), properties);
    }

    static class PropertiesBuilder {

        private Map<String, Object> properties = new HashMap<String, Object>();

        public PropertiesBuilder put(String key, Object value) {
            properties.put(key, value);
            return this;
        }

        public Map<String, Object> build() {
            return properties;
        }
    }
}
