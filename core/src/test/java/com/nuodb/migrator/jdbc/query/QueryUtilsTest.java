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
package com.nuodb.migrator.jdbc.query;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.query.QueryUtils.*;
import static java.util.Collections.emptySet;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class QueryUtilsTest {

    @DataProvider(name = "where")
    public Object[][] getWhereData() {
        return new Object[][]{
                {"SELECT \"column1\", \"column2\" FROM \"table\"", emptySet(),
                        AND, "SELECT \"column1\", \"column2\" FROM \"table\""},
                {"SELECT \"column1\", \"column2\" FROM \"table\"", newArrayList("\"column1\" > \"column2\""),
                        AND, "SELECT \"column1\", \"column2\" FROM \"table\" WHERE \"column1\" > \"column2\""},
                {"SELECT \"column1\", \"column2\" FROM \"table\"", newArrayList("\"column1\" > \"column2\"", "\"column1\" > 0"),
                        OR, "SELECT \"column1\", \"column2\" FROM \"table\" WHERE \"column1\" > \"column2\" OR \"column1\" > 0"}
        };
    }

    @Test(dataProvider = "where")
    public void testWhere(String query, Collection<String> filters, String operator, String queryWhere) {
        assertEquals(where(query, filters, operator), queryWhere);
    }

    @DataProvider(name = "orderBy")
    public Object[][] getOrderByData() {
        return new Object[][]{
                {"SELECT \"column1\", \"column2\" FROM \"table\"", emptySet(),
                        null, "SELECT \"column1\", \"column2\" FROM \"table\""},
                {"SELECT \"column1\", \"column2\" FROM \"table\"", newArrayList("\"column1\""),
                        null, "SELECT \"column1\", \"column2\" FROM \"table\" ORDER BY \"column1\""},
                {"SELECT \"column1\", \"column2\" FROM \"table\"", newArrayList("\"column1\"", "\"column2\""),
                        ASC, "SELECT \"column1\", \"column2\" FROM \"table\" ORDER BY \"column1\", \"column2\" ASC"},
        };
    }

    @Test(dataProvider = "orderBy")
    public void testOrderBy(String query, Collection<String> columns, String order, String queryOrderBy) {
        assertEquals(orderBy(query, columns, order), queryOrderBy);
    }
}
