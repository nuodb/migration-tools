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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.google.common.collect.HashMultimap;
import com.nuodb.migrator.jdbc.dialect.DB2Dialect;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.MySQLDialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect206;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect256;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect320;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect340;
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Check;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager.TABLES;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager.FOREIGN_KEYS;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager.UNIQUE_CONSTRAINTS;
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
        scriptGeneratorManager.addAttribute(TABLES, newLinkedHashSet());
        scriptGeneratorManager.addAttribute(FOREIGN_KEYS, HashMultimap.create());
        scriptGeneratorManager.addAttribute(UNIQUE_CONSTRAINTS, true);
    }

    public Object[][] createGetScriptsData(boolean checkTableLocks) {
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
        f1_2.setSize(32L);
        f1_2.setNullable(false);
        f1_2.setPosition(2);

        PrimaryKey pk1_1 = new PrimaryKey(valueOf("users_id"));
        pk1_1.addColumn(f1_1, 1);
        table1.setPrimaryKey(pk1_1);

        Collection<Object[]> data = newArrayList();
        data.add(new Object[] { table1, newArrayList(DROP), null,
                newArrayList(new Script("DROP TABLE IF EXISTS \"users\" CASCADE")) });
        data.add(new Object[] { table1, newArrayList(CREATE), null,
                newArrayList(
                        new Script("CREATE TABLE \"users\" (\"id\" INTEGER NOT NULL, \"login\" VARCHAR(32) NOT NULL)"),
                        new Script("ALTER TABLE \"users\" ADD CONSTRAINT \"users_id\" PRIMARY KEY (\"id\")",
                                new Table("users"), checkTableLocks)) });

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
        f2_3.setSize(100L);
        table2_2.addColumn(f2_3);
        catalog2.addSchema(valueOf("s2")).addTable(table2_2);

        data.add(new Object[] { database2, newArrayList(CREATE), null,
                newArrayList(new Script("USE \"s1\""), new Script("CREATE TABLE \"t1\" (\"id\" BIGINT NOT NULL)"),
                        new Script("USE \"s2\""), new Script("CREATE TABLE \"t2\" (\"id\" VARCHAR(100) NOT NULL)")) });

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

        Index index4_1 = new Index("idx_4_1");
        index4_1.addColumn(f4_1, 1);
        table4.addIndex(index4_1);

        Index index4_2 = new Index("idx_4_2");
        index4_2.addColumn(f4_1, 1);
        index4_2.addColumn(f4_3, 2);
        table4.addIndex(index4_2);

        Index index4_3 = new Index("idx_4_3");
        index4_3.addColumn(f4_3, 1);
        table4.addIndex(index4_3);

        catalog4.addSchema((String) null).addTable(table4);

        Database database4_2 = new Database();
        NuoDBDialect206 targetDialect4 = new NuoDBDialect206();
        database4_2.setDialect(targetDialect4);

        data.add(new Object[] { database4_11, newArrayList(CREATE), targetDialect4,
                newArrayList(new Script("USE \"c1\""), new Script(
                        "CREATE TABLE \"t1\" (\"f1\" VARCHAR(0) NOT NULL, \"f2\" FLOAT NOT NULL, \"f3\" DATE NOT NULL)"),
                        // comma concatenated indexes
                        new Script(
                                "CREATE INDEX \"idx_t1_idx_4_1\" ON \"t1\" (\"f1\"), "
                                        + "CREATE INDEX \"idx_t1_idx_4_2\" ON \"t1\" (\"f1\", \"f3\"), "
                                        + "CREATE INDEX \"idx_t1_idx_4_3\" ON \"t1\" (\"f3\")",
                                new Table("t1"), checkTableLocks)) });

        Database nuodatabase = new Database();
        Catalog nuocatalog = nuodatabase.addCatalog(valueOf(null));

        // create table xxx (c1 int, c2 int, constraint idx1 unique (c1, c2));
        Table nuotable1 = new Table("xxx");
        nuotable1.setDatabase(nuodatabase);

        Column nuoc1 = nuotable1.addColumn("c1");
        nuoc1.setTypeCode(INTEGER);
        nuoc1.setTypeName("INTEGER");
        nuoc1.setNullable(true);
        nuoc1.setPosition(1);

        Column nuoc2 = nuotable1.addColumn("c2");
        nuoc2.setTypeCode(INTEGER);
        nuoc2.setTypeName("INTEGER");
        nuoc2.setNullable(true);
        nuoc2.setPosition(2);

        Index nuoidx1 = new Index(valueOf("idx1"));
        nuoidx1.addColumn(nuoc1, 1);
        nuoidx1.addColumn(nuoc2, 2);
        nuoidx1.setUnique(true);
        nuoidx1.setUniqueConstraint(true);
        nuotable1.addIndex(nuoidx1);

        nuocatalog.addSchema(valueOf("USER")).addTable(nuotable1);
        data.add(new Object[] { nuotable1, newArrayList(DROP), null,
                newArrayList(new Script("DROP TABLE IF EXISTS \"xxx\" CASCADE")) });
        data.add(new Object[] { nuotable1, newArrayList(CREATE), null,
                newArrayList(new Script("CREATE TABLE \"xxx\" (\"c1\" INTEGER, "
                        + "\"c2\" INTEGER, CONSTRAINT \"idx_xxx_idx1\" UNIQUE (\"c1\", \"c2\"))")) });

        // create table xxx2 (c1 int check(c1 > 10), c2 int, constraint even
        // check (c1 % 2 = 0));
        Table nuotable2 = new Table("xxx2");
        nuotable2.setDatabase(nuodatabase);

        Column nuoc1_2 = nuotable2.addColumn("c1");
        nuoc1_2.setTypeCode(INTEGER);
        nuoc1_2.setTypeName("INTEGER");
        nuoc1_2.setNullable(true);
        nuoc1_2.setPosition(1);

        Column nuoc2_2 = nuotable2.addColumn("c2");
        nuoc2_2.setTypeCode(INTEGER);
        nuoc2_2.setTypeName("INTEGER");
        nuoc2_2.setNullable(true);
        nuoc2_2.setPosition(2);

        nuoc1_2.addCheck(new Check(valueOf("c1"), "c1 > 10"));
        nuotable2.addCheck(new Check(valueOf("even"), "c1 % 2 = 0"));

        nuocatalog.addSchema(valueOf("USER")).addTable(nuotable2);
        data.add(new Object[] { nuotable2, newArrayList(DROP), null,
                newArrayList(new Script("DROP TABLE IF EXISTS \"xxx2\" CASCADE")) });
        data.add(new Object[] { nuotable2, newArrayList(CREATE), null,
                newArrayList(new Script("CREATE TABLE \"xxx2\" (\"c1\" INTEGER CHECK(c1 > 10), "
                        + "\"c2\" INTEGER, CONSTRAINT \"even\" CHECK(c1 % 2 = 0))")) });

        // create table xxx3 (c1 int, c2 int, constraint idx3 key abc(c1, c2));
        Table nuotable3 = new Table("xxx3");
        nuotable3.setDatabase(nuodatabase);

        Column nuoc1_3 = nuotable3.addColumn("c1");
        nuoc1_3.setTypeCode(INTEGER);
        nuoc1_3.setTypeName("INTEGER");
        nuoc1_3.setNullable(true);
        nuoc1_3.setPosition(1);

        Column nuoc2_3 = nuotable3.addColumn("c2");
        nuoc2_3.setTypeCode(INTEGER);
        nuoc2_3.setTypeName("INTEGER");
        nuoc2_3.setNullable(true);
        nuoc2_3.setPosition(2);

        Index nuoidx3 = new Index(valueOf("idx3"));
        nuoidx3.addColumn(nuoc1_3, 1);
        nuoidx3.addColumn(nuoc2_3, 2);
        // idx3 is neither a unique index nor unique constraint.
        // it is a secondary index, i.e., a normal index
        nuoidx3.setUnique(false);
        nuoidx3.setUniqueConstraint(false);
        nuotable3.addIndex(nuoidx3);

        nuocatalog.addSchema(valueOf("USER")).addTable(nuotable3);
        data.add(new Object[] { nuotable3, newArrayList(DROP), null,
                newArrayList(new Script("DROP TABLE IF EXISTS \"xxx3\" CASCADE")) });
        data.add(new Object[] { nuotable3, newArrayList(CREATE), null,
                newArrayList(new Script("CREATE TABLE \"xxx3\" (\"c1\" INTEGER, " + "\"c2\" INTEGER)"),
                        new Script("CREATE INDEX \"idx_xxx3_idx3\" ON \"xxx3\" (\"c1\", \"c2\")", new Table("xxx3"),
                                checkTableLocks)) });

        // create table xxx4 (c1 int, c2 int, constraint idx4 unique key cdf(c1,
        // c2), constraint idx4_2 unique (c2));
        Table nuotable4 = new Table("xxx4");
        nuotable4.setDatabase(nuodatabase);

        Column nuoc1_4 = nuotable4.addColumn("c1");
        nuoc1_4.setTypeCode(INTEGER);
        nuoc1_4.setTypeName("INTEGER");
        nuoc1_4.setNullable(true);
        nuoc1_4.setPosition(1);

        Column nuoc2_4 = nuotable4.addColumn("c2");
        nuoc2_4.setTypeCode(INTEGER);
        nuoc2_4.setTypeName("INTEGER");
        nuoc2_4.setNullable(true);
        nuoc2_4.setPosition(2);

        Index nuoidx4 = new Index(valueOf("idx4"));
        nuoidx4.addColumn(nuoc1_4, 1);
        nuoidx4.addColumn(nuoc2_4, 2);
        // idx4 is a unique index NOT unique constraint,
        nuoidx4.setUnique(true);
        nuoidx4.setUniqueConstraint(false);
        nuotable4.addIndex(nuoidx4);

        Index nuoidx4_2 = new Index(valueOf("idx4_2"));
        nuoidx4_2.addColumn(nuoc2_4, 2);
        // idx4_2 is a unique constraint NOT unique index.
        // both unique constraint and unique index are Unique
        nuoidx4_2.setUnique(true);
        nuoidx4_2.setUniqueConstraint(true);
        nuotable4.addIndex(nuoidx4_2);

        nuocatalog.addSchema(valueOf("USER")).addTable(nuotable4);
        data.add(new Object[] { nuotable4, newArrayList(DROP), null,
                newArrayList(new Script("DROP TABLE IF EXISTS \"xxx4\" CASCADE")) });
        data.add(new Object[] { nuotable4, newArrayList(CREATE), null,
                newArrayList(new Script("CREATE TABLE \"xxx4\" (\"c1\" INTEGER, \"c2\" INTEGER UNIQUE)"),
                        new Script("CREATE UNIQUE INDEX \"idx_xxx4_idx4\" ON \"xxx4\" (\"c1\", \"c2\")",
                                new Table("xxx4"), checkTableLocks)) });

        Catalog nuocatalog2 = nuodatabase.addCatalog(valueOf("FKTest"));

        // create table xxx5 (c1_5 int, c2_5 int, constraint pk5 primary
        // key(c2_5), constraint fk5 foreign key (c1_5) references xxx6(c1_6));
        Table nuotable5 = new Table("xxx5");
        nuotable3.setDatabase(nuodatabase);

        Column nuoc1_5 = nuotable5.addColumn("c1_5");
        nuoc1_5.setTypeCode(INTEGER);
        nuoc1_5.setTypeName("INTEGER");
        nuoc1_5.setNullable(true);
        nuoc1_5.setPosition(1);

        Column nuoc2_5 = nuotable5.addColumn("c2_5");
        nuoc2_5.setTypeCode(INTEGER);
        nuoc2_5.setTypeName("INTEGER");
        nuoc2_5.setNullable(true);
        nuoc2_5.setPosition(2);

        PrimaryKey pk5 = new PrimaryKey(valueOf("pk5"));
        pk5.addColumn(nuoc2_5, 2);
        nuotable5.setPrimaryKey(pk5);

        // create table xxx6 (c1_6 int, c2_6 int, constraint pk6 primary key
        // (c1_6), constraint fk6 foreign key (c2_6) references xxx5(c2_5));
        Table nuotable6 = new Table("xxx6");
        nuotable3.setDatabase(nuodatabase);

        Column nuoc1_6 = nuotable6.addColumn("c1_6");
        nuoc1_6.setTypeCode(INTEGER);
        nuoc1_6.setTypeName("INTEGER");
        nuoc1_6.setNullable(true);
        nuoc1_6.setPosition(1);

        Column nuoc2_6 = nuotable6.addColumn("c2_6");
        nuoc2_6.setTypeCode(INTEGER);
        nuoc2_6.setTypeName("INTEGER");
        nuoc2_6.setNullable(true);
        nuoc2_6.setPosition(2);

        PrimaryKey pk6 = new PrimaryKey(valueOf("pk6"));
        pk6.addColumn(nuoc1_6, 1);
        nuotable6.setPrimaryKey(pk6);

        // Create the foreign key (for nuotable5) that references the table
        // created later (nuotable6)
        ForeignKey nuoidx5 = new ForeignKey(valueOf("fk5"));
        nuoidx5.setPrimaryTable(nuotable6);
        nuoidx5.setForeignTable(nuotable5);
        nuoidx5.addReference(nuoc1_6, nuoc1_5, 1);
        nuotable5.addForeignKey(nuoidx5);

        ForeignKey nuoidx6 = new ForeignKey(valueOf("fk6"));
        nuoidx6.setPrimaryTable(nuotable5);
        nuoidx6.setForeignTable(nuotable6);
        nuoidx6.addReference(nuoc2_5, nuoc2_6, 2);
        nuotable6.addForeignKey(nuoidx6);

        nuocatalog2.addSchema(valueOf("FKTest")).addTable(nuotable5);
        nuocatalog2.addSchema(valueOf("FKTest")).addTable(nuotable6);

        // create table xxx7 (c1_7 int, constraint fk7 foreign key (c1_7)
        // references xxx6(c1_6));
        Table nuotable7 = new Table("xxx7");
        Column nuoc1_7 = nuotable7.addColumn("c1_7");
        nuoc1_7.setTypeCode(INTEGER);
        nuoc1_7.setTypeName("INTEGER");
        nuoc1_7.setNullable(true);
        nuoc1_7.setPosition(1);

        ForeignKey nuoidx7 = new ForeignKey(valueOf("fk7"));
        nuoidx7.setPrimaryTable(nuotable6);
        nuoidx7.setForeignTable(nuotable7);
        nuoidx7.addReference(nuoc1_6, nuoc1_7, 1);
        nuotable7.addForeignKey(nuoidx7);

        nuocatalog2.addSchema(valueOf("FKTest")).addTable(nuotable7);
        data.add(new Object[] { nuocatalog2, newArrayList(DROP, CREATE), null, newArrayList(
                new Script("USE \"FKTest\""), new Script("DROP TABLE IF EXISTS \"xxx5\" CASCADE"),
                new Script("CREATE TABLE \"xxx5\" (\"c1_5\" INTEGER, \"c2_5\" INTEGER)"),
                new Script("ALTER TABLE \"xxx5\" ADD CONSTRAINT \"pk5\" PRIMARY KEY (\"c2_5\")", new Table("xxx5"),
                        checkTableLocks),
                new Script("DROP TABLE IF EXISTS \"xxx6\" CASCADE"),
                new Script("CREATE TABLE \"xxx6\" (\"c1_6\" INTEGER, \"c2_6\" INTEGER)"),
                new Script("ALTER TABLE \"xxx6\" ADD CONSTRAINT \"pk6\" PRIMARY KEY (\"c1_6\")", new Table("xxx6"),
                        checkTableLocks),
                new Script(
                        "ALTER TABLE \"xxx6\" ADD CONSTRAINT \"fk6\" FOREIGN KEY (\"c2_6\") REFERENCES \"FKTest\".\"xxx5\" (\"c2_5\")",
                        new Table("xxx6"), checkTableLocks),
                new Script("DROP TABLE IF EXISTS \"xxx7\" CASCADE"),
                new Script("CREATE TABLE \"xxx7\" (\"c1_7\" INTEGER)"),
                new Script(
                        "ALTER TABLE \"xxx7\" ADD CONSTRAINT \"fk7\" FOREIGN KEY (\"c1_7\") REFERENCES \"FKTest\".\"xxx6\" (\"c1_6\")",
                        new Table("xxx7"), checkTableLocks),
                new Script(
                        "ALTER TABLE \"xxx5\" ADD CONSTRAINT \"fk5\" FOREIGN KEY (\"c1_5\") REFERENCES \"FKTest\".\"xxx6\" (\"c1_6\")",
                        new Table("xxx5"), checkTableLocks)) });

        return data.toArray(new Object[][] {});
    }

    @Test
    public void testGetScriptsMySQL() throws Exception {
        boolean[] vals = { true, false };
        for (boolean checkTableLocks : vals) {
            Object[][] data = createGetScriptsData(checkTableLocks);
            for (Object[] obj : data) {
                MetaData object = (MetaData) obj[0];
                Collection<ScriptType> scriptTypes = (Collection<ScriptType>) obj[1];
                Dialect targetDialect = (Dialect) obj[2];
                Collection<Script> expected = (Collection<Script>) obj[3];

                scriptGeneratorManager.setSourceSession(createSession(new MySQLDialect(MYSQL)));
                scriptGeneratorManager.setTargetDialect(createTargetDialect(checkTableLocks));
                scriptGeneratorManager.setScriptTypes(scriptTypes);

                Collection<Script> actualScripts = scriptGeneratorManager.getScripts(object);
                assertNotNull(actualScripts);
                assertEquals(newArrayList(actualScripts), newArrayList(expected));
            }
        }
    }

    @Test
    public void testGetScriptsNuoDB() throws Exception {
        boolean[] vals = { true, false };
        for (boolean checkTableLocks : vals) {
            Object[][] data = createGetScriptsData(checkTableLocks);
            for (Object[] obj : data) {
                MetaData object = (MetaData) obj[0];
                Collection<ScriptType> scriptTypes = (Collection<ScriptType>) obj[1];
                Dialect targetDialect = (Dialect) obj[2];
                Collection<Script> expected = (Collection<Script>) obj[3];
                scriptGeneratorManager.setSourceSession(createSession(new NuoDBDialect256()));
                scriptGeneratorManager.setTargetDialect(createTargetDialect(checkTableLocks));
                scriptGeneratorManager.setScriptTypes(scriptTypes);

                Collection<Script> actualScripts = scriptGeneratorManager.getScripts(object);
                assertNotNull(actualScripts);
                assertEquals(newArrayList(actualScripts), newArrayList(expected));
            }
        }
    }

    private Dialect createTargetDialect(boolean checkTableLocks) {
        NuoDBDialect dialect;
        if (checkTableLocks) {
            dialect = new NuoDBDialect320();
        } else {
            dialect = new NuoDBDialect340();
        }
        Database database = new Database();
        database.setDialect(dialect);
        return dialect;
    }
}
