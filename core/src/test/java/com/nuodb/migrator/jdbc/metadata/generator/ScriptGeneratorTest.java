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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.google.common.collect.Lists;
import com.nuodb.migrator.jdbc.dialect.MySQLDialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
public class ScriptGeneratorTest {

    private ScriptGeneratorContext scriptGeneratorContext;

    @BeforeMethod
    public void setUp() {
        NuoDBDialect dialect = new NuoDBDialect();

        Database database = new Database();
        database.setDialect(dialect);

        scriptGeneratorContext = new ScriptGeneratorContext();
        scriptGeneratorContext.setDialect(dialect);
    }

    @DataProvider(name = "getScriptsData")
    public Object[][] createGetScriptsData() {
        Database database = new Database();
        database.setDialect(new MySQLDialect(new DatabaseInfo("MySQL")));

        Table table = new Table("users");
        table.setDatabase(database);

        Column id = table.addColumn("id");
        id.setTypeCode(Types.INTEGER);
        id.setTypeName("integer");
        id.setNullable(false);
        id.setPosition(1);

        Column login = table.addColumn("login");
        login.setTypeCode(Types.VARCHAR);
        login.setTypeName("varchar");
        login.setSize(32);
        login.setNullable(false);
        login.setPosition(2);

        PrimaryKey primaryKey = new PrimaryKey(valueOf("users_id"));
        primaryKey.addColumn(id, 1);
        table.setPrimaryKey(primaryKey);

        Collection<Object[]> data = Lists.newArrayList();
        data.add(new Object[]{table, newArrayList(ScriptType.DROP),
                newArrayList("DROP TABLE IF EXISTS \"users\" CASCADE")});
        data.add(new Object[]{table, newArrayList(ScriptType.CREATE),
                newArrayList(
                "CREATE TABLE \"users\" (\"id\" INTEGER NOT NULL, " +
                        "\"login\" VARCHAR(32) NOT NULL, PRIMARY KEY (\"id\"))")});

        return data.toArray(new Object[][]{});
    }

    @Test(dataProvider = "getScriptsData")
    public void testGetScripts(MetaData object, Collection<ScriptType> scriptTypes,
                               Collection<String> expected) throws Exception {
        scriptGeneratorContext.setScriptTypes(scriptTypes);

        Collection<String> scripts = scriptGeneratorContext.getScripts(object);
        assertNotNull(scripts);
        assertEquals(newArrayList(scripts), newArrayList(expected));
    }
}
