/**
 * Copyright (c) 2014, NuoDB, Inc.
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
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.MySQLDialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect206;
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.DROP;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static java.sql.Types.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
public class ScriptGeneratorTest {

    private ScriptGeneratorManager scriptGeneratorManager;

    @BeforeMethod
    public void setUp() throws SQLException {
        scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.setSourceSession(createSession(new MySQLDialect(MYSQL)));
    }

    @DataProvider(name = "getScripts")
    public Object[][] createGetScriptsData() {
        // create & drop scripts from a source mysql table
        Database database1 = new Database();
        database1.setDialect(new MySQLDialect(new DatabaseInfo("MySQL")));

        Table table1 = new Table("users");
        table1.setDatabase(database1);

        Column f1_1 = table1.addColumn("id");
        f1_1.setTypeCode(INTEGER);
        f1_1.setTypeName("INTEGER");
        f1_1.setNullable(false);
        f1_1.setPosition(1);

        Column f1_2 = table1.addColumn("login");
        f1_2.setTypeCode(Types.VARCHAR);
        f1_2.setTypeName("VARCHAR");
        f1_2.setSize(32);
        f1_2.setNullable(false);
        f1_2.setPosition(2);

        PrimaryKey pk1_1 = new PrimaryKey(valueOf("users_id"));
        pk1_1.addColumn(f1_1, 1);
        table1.setPrimaryKey(pk1_1);

        Collection<Object[]> data = newArrayList();
        data.add(new Object[]{table1, newArrayList(DROP), null,
                newArrayList("DROP TABLE IF EXISTS \"users\" CASCADE")});
        data.add(new Object[]{table1, newArrayList(CREATE), null,
                newArrayList(
                        "CREATE TABLE \"users\" (\"id\" INTEGER NOT NULL, " +
                                "\"login\" VARCHAR(32) NOT NULL, PRIMARY KEY (\"id\"))")});

        // test db2 multiple schemas
        Database database2 = new Database();
        database2.setDialect(new DB2Dialect(new DatabaseInfo("DB2/DARWIN")));
        Catalog catalog2 = database2.addCatalog(valueOf(null));

        Table table2_1 = new Table("t1");
        table2_1.setDatabase(database2);
        Column f2_1 = table2_1.addColumn("id");
        f2_1.setTypeCode(BIGINT);
        f2_1.setTypeName("BIGINT");
        table2_1.addColumn(f2_1);
        catalog2.addSchema(valueOf("s1")).addTable(table2_1);

        Table table2_2 = new Table("t2");
        table2_2.setDatabase(database2);

        Column f2_3 = table2_2.addColumn("id");
        f2_3.setTypeCode(VARCHAR);
        f2_3.setTypeName("VARCHAR");
        f2_3.setSize(100);
        table2_2.addColumn(f2_3);
        catalog2.addSchema(valueOf("s2")).addTable(table2_2);

        data.add(new Object[]{database2, newArrayList(CREATE), null,
                newArrayList(
                        "USE \"s1\"", "CREATE TABLE \"t1\" (\"id\" BIGINT NOT NULL)",
                        "USE \"s2\"", "CREATE TABLE \"t2\" (\"id\" VARCHAR(100) NOT NULL)")
                });


        // test create indexes grouping
        Database database4_11 = new Database();
        database4_11.setDialect(new MySQLDialect(new DatabaseInfo("MySQL")));
        Catalog catalog4 = database4_11.addCatalog(valueOf("c1"));
        Table table4 = new Table("t1");

        Column f4_1 = table4.addColumn("f1");
        f4_1.setTypeCode(VARCHAR);
        f4_1.setTypeName("VARCHAR");
        table4.addColumn(f4_1);

        Column f4_2 = table4.addColumn("f2");
        f4_2.setTypeCode(FLOAT);
        f4_2.setTypeName("FLOAT");
        table4.addColumn(f4_2);

        Column f4_3 = table4.addColumn("f3");
        f4_3.setTypeCode(DATE);
        f4_3.setTypeName("DATE");
        table4.addColumn(f4_3);

        Index index4_1 = new Index("4_1");
        index4_1.addColumn(f4_1, 1);
        table4.addIndex(index4_1);

        Index index4_2 = new Index("4_2");
        index4_2.addColumn(f4_1, 1);
        index4_2.addColumn(f4_3, 2);
        table4.addIndex(index4_2);

        Index index4_3 = new Index("4_3");
        index4_3.addColumn(f4_3, 1);
        table4.addIndex(index4_3);

        catalog4.addSchema((String) null).addTable(table4);

        Database database4_2 = new Database();
        NuoDBDialect206 targetDialect4 = new NuoDBDialect206();
        database4_2.setDialect(targetDialect4);

        data.add(new Object[]{database4_11, newArrayList(CREATE), targetDialect4,
                newArrayList(
                        "USE \"c1\"",
                        "CREATE TABLE \"t1\" (\"f1\" VARCHAR(0) NOT NULL, \"f2\" FLOAT NOT NULL, \"f3\" DATE NOT NULL)",
                        // comma concatenated indexes
                        "CREATE INDEX \"idx_t1_4_1\" ON \"t1\" (\"f1\"), " +
                        "CREATE INDEX \"idx_t1_4_2\" ON \"t1\" (\"f1\", \"f3\"), " +
                        "CREATE INDEX \"idx_t1_4_3\" ON \"t1\" (\"f3\")"
                )});
        return data.toArray(new Object[][]{});
    }

    @Test(dataProvider = "getScripts")
    public void testGetScripts(MetaData object, Collection<ScriptType> scriptTypes, Dialect targetDialect,
                               Collection<String> expected) throws Exception {
        scriptGeneratorManager.setTargetDialect(targetDialect == null ? createTargetDialect() : targetDialect);
        scriptGeneratorManager.setScriptTypes(scriptTypes);

        Collection<String> scripts = scriptGeneratorManager.getScripts(object);
        assertNotNull(scripts);
        assertEquals(newArrayList(expected), newArrayList(scripts));
    }

    private Dialect createTargetDialect() {
        NuoDBDialect dialect = new NuoDBDialect();
        Database database = new Database();
        database.setDialect(dialect);
        return dialect;
    }
}
