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
package com.nuodb.migrator.jdbc.query;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.metadata.Table;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createTable;
import static com.nuodb.migrator.jdbc.query.InsertType.INSERT;
import static com.nuodb.migrator.jdbc.query.InsertType.REPLACE;
import static java.util.Collections.emptySet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
public class QueryBuilderTest {

    @DataProvider(name = "insertQueryBuilder")
    public Object[][] createInsertQueryBuilderData() {
        Table table = createTable(null, "schema", "table");
        table.addColumn("column1");
        table.addColumn("column2");
        Dialect dialect = new NuoDBDialect();
        return new Object[][] {
                { table, dialect, false, INSERT, "INSERT INTO \"table\" (\"column1\", \"column2\") VALUES (?, ?)" },
                { table, dialect, false, INSERT, "INSERT INTO \"table\" (\"column1\", \"column2\") VALUES (?, ?)" },
                { table, dialect, true, INSERT,
                        "INSERT INTO \"schema\".\"table\" (\"column1\", \"column2\") VALUES (?, ?)" },
                { table, dialect, true, REPLACE,
                        "REPLACE INTO \"schema\".\"table\" (\"column1\", \"column2\") VALUES (?, ?)" } };
    }

    @Test(dataProvider = "insertQueryBuilder")
    public void testInsertQueryBuilder(Table table, Dialect dialect, boolean qualifyNames, InsertType insertType,
            String query) {
        InsertQueryBuilder insertQueryBuilder = new InsertQueryBuilder();
        insertQueryBuilder.into(table);
        insertQueryBuilder.dialect(dialect);
        insertQueryBuilder.qualifyNames(qualifyNames);
        insertQueryBuilder.insertType(insertType);
        InsertQuery insertQuery = insertQueryBuilder.build();

        assertNotNull(insertQuery);
        assertEquals(insertQuery.getInsertType(), insertType);
        assertEquals(insertQuery.getDialect(), dialect);
        assertEquals(insertQuery.getColumns().keySet(), table.getColumns());
        assertEquals(insertQuery.toString(), query);
    }

    @DataProvider(name = "selectQueryBuilder")
    public Object[][] createSelectQueryBuilderData() {
        Table table = createTable(null, "schema", "table");
        table.addColumn("column1");
        table.addColumn("column2");
        Dialect dialect = new NuoDBDialect();
        return new Object[][] { { table, dialect, false, emptySet(), "SELECT \"column1\", \"column2\" FROM \"table\"" },
                { table, dialect, true, emptySet(), "SELECT \"column1\", \"column2\" FROM \"schema\".\"table\"" },
                { table, dialect, false, newArrayList("\"column1\">0"),
                        "SELECT \"column1\", \"column2\" FROM \"table\" WHERE \"column1\">0" },
                { table, dialect, false, newArrayList("\"column1\">0", "\"column2\"<0"),
                        "SELECT \"column1\", \"column2\" FROM \"table\" WHERE \"column1\">0 AND \"column2\"<0" } };
    }

    @Test(dataProvider = "selectQueryBuilder")
    public void testSelectQueryBuilder(Table table, Dialect dialect, boolean qualifyNames, Collection<String> filters,
            String query) {
        SelectQuery selectQuery = new SelectQueryBuilder().dialect(dialect).qualifyNames(qualifyNames).from(table)
                .filters(filters).build();

        assertNotNull(selectQuery);
        assertEquals(selectQuery.getDialect(), dialect);
        assertEquals(selectQuery.getColumns(), table.getColumns());
        assertEquals(selectQuery.toString(), query);
    }
}
