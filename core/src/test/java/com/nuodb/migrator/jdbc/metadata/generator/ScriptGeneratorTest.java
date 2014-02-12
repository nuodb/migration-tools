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
import com.nuodb.migrator.jdbc.dialect.DB2Dialect;
import com.nuodb.migrator.jdbc.dialect.MySQLDialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.metadata.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
public class ScriptGeneratorTest {

    private ScriptGeneratorManager scriptGeneratorManager;

    @BeforeMethod
    public void setUp() throws SQLException {
        NuoDBDialect dialect = new NuoDBDialect();

        Database database = new Database();
        database.setDialect(dialect);

        scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.setTargetDialect(dialect);
        scriptGeneratorManager.setSourceSession(createSession(new MySQLDialect(MYSQL)));
    }

    @DataProvider(name = "getScripts")
    public Object[][] createGetScriptsData() {
        // create & drop scripts from a source mysql table
        Database database1 = new Database();
        database1.setDialect(new MySQLDialect(new DatabaseInfo("MySQL")));

        Table table = new Table("users");
        table.setDatabase(database1);

        Column id = table.addColumn("id");
        id.setTypeCode(Types.INTEGER);
        id.setTypeName("INTEGER");
        id.setNullable(false);
        id.setPosition(1);

        Column login = table.addColumn("login");
        login.setTypeCode(Types.VARCHAR);
        login.setTypeName("VARCHAR");
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

        // test db2 multiple schemas
        Database database2 = new Database();
        database2.setDialect(new DB2Dialect(new DatabaseInfo("DB2/DARWIN")));
        Catalog catalog1 = database2.addCatalog(valueOf(null));

        Table table1 = new Table("t1");
        table1.setDatabase(database2);
        Column id1 = table.addColumn("id");
        id.setTypeCode(Types.INTEGER);
        id.setTypeName("INTEGER");
        table1.addColumn(id1);
        catalog1.addSchema(valueOf("s1")).addTable(table1);

        Table table2 = new Table("t1");
        table2.setDatabase(database2);

        Column id2 = table.addColumn("id");
        id.setTypeCode(Types.INTEGER);
        id.setTypeName("INTEGER");
        table2.addColumn(id2);
        catalog1.addSchema(valueOf("s2")).addTable(table2);

        data.add(new Object[]{database2, newArrayList(ScriptType.CREATE),
                newArrayList(
                        "USE \"s1\"", "CREATE TABLE \"t1\" (\"id\" INTEGER NOT NULL)",
                        "USE \"s2\"", "CREATE TABLE \"t1\" (\"id\" INTEGER NOT NULL)")
        });
        return data.toArray(new Object[][]{});
    }

    @Test(dataProvider = "getScripts")
    public void testGetScripts(MetaData object, Collection<ScriptType> scriptTypes,
                               Collection<String> expected) throws Exception {
        scriptGeneratorManager.setScriptTypes(scriptTypes);

        Collection<String> scripts = scriptGeneratorManager.getScripts(object);
        assertNotNull(scripts);
        assertEquals(newArrayList(expected), newArrayList(scripts));
    }
}
