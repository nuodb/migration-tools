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

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.MySQLDialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect206;
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Collection;
import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migrator.jdbc.metadata.IndexUtils.getCreateMultipleIndexes;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static java.sql.Types.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * @author Mukund
 */
public class IndexUtilsTest {

    private ScriptGeneratorManager scriptGeneratorManager;

    @BeforeMethod
    public void setUp() throws SQLException {
        scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.setSourceSession(createSession(new MySQLDialect(MYSQL)));
    }

    @DataProvider(name = "getIndexScripts")
    public Object[][] createIndexScriptsData() {
        Database database4_11 = new Database();
        database4_11.setDialect(new MySQLDialect(new DatabaseInfo("MySQL")));
        Catalog catalog4 = database4_11.addCatalog(valueOf("c1"));
        Table table = new Table("t1");

        Column c1_1 = table.addColumn("c1");
        c1_1.setTypeCode(VARCHAR);
        c1_1.setTypeName("VARCHAR");
        table.addColumn(c1_1);

        Column c2_2 = table.addColumn("c2");
        c2_2.setTypeCode(VARCHAR);
        c2_2.setTypeName("VARCHAR");
        table.addColumn(c2_2);

        Column c3_3 = table.addColumn("c3");
        c3_3.setTypeCode(DATE);
        c3_3.setTypeName("DATE");
        table.addColumn(c3_3);

        Column c4_4 = table.addColumn("c4");
        c4_4.setTypeCode(VARCHAR);
        c4_4.setTypeName("VARCHAR");
        table.addColumn(c4_4);

        Column c5_5 = table.addColumn("c5");
        c5_5.setTypeCode(VARCHAR);
        c5_5.setTypeName("VARCHAR");
        table.addColumn(c5_5);

        Index index1_1 = new Index("idx_1_1");
        index1_1.setType("FULLTEXT");
        index1_1.addColumn(c1_1, 1);
        table.addIndex(index1_1);

        Index index1_2 = new Index("idx_1_2");
        index1_2.setType("BTREE");
        index1_2.addColumn(c2_2, 1);
        index1_2.addColumn(c3_3, 2);
        table.addIndex(index1_2);

        Index index1_3 = new Index("idx_1_3");
        index1_3.setType("BTREE");
        index1_3.addColumn(c3_3, 1);
        table.addIndex(index1_3);

        Index index1_4 = new Index("idx_1_4");
        index1_4.setType("SPATIAL");
        index1_4.addColumn(c4_4, 1);
        table.addIndex(index1_4);

        Index index1_5 = new Index("idx_1_5");
        index1_5.addColumn(c5_5, 1);
        table.addIndex(index1_5);

        catalog4.addSchema((String) null).addTable(table);

        Database database4_2 = new Database();
        NuoDBDialect206 targetDialect4 = new NuoDBDialect206();
        database4_2.setDialect(targetDialect4);

        Collection<Object[]> data = newArrayList();
        Collection<Index> indexes = newArrayList();
        indexes.add(index1_1);
        indexes.add(index1_2);
        indexes.add(index1_3);
        indexes.add(index1_4);
        indexes.add(index1_5);
        data.add(new Object[] { indexes, newArrayList(CREATE), targetDialect4,
                newArrayList("CREATE INDEX \"idx_t1_idx_1_2\" ON \"t1\" (\"c2\", \"c3\"), "
                        + "CREATE INDEX \"idx_t1_idx_1_3\" ON \"t1\" (\"c3\"), "
                        + "CREATE INDEX \"idx_t1_idx_1_5\" ON \"t1\" (\"c5\")") });
        return data.toArray(new Object[][] {});
    }

    @Test(dataProvider = "getIndexScripts")
    public void testIndexScripts(Collection<Index> indexes, Collection<ScriptType> scriptTypes, Dialect targetDialect,
            Collection<String> expected) throws Exception {
        scriptGeneratorManager.setTargetDialect(targetDialect == null ? createTargetDialect() : targetDialect);
        scriptGeneratorManager.setScriptTypes(scriptTypes);

        Collection<Script> actualScripts = getCreateMultipleIndexes(indexes, scriptGeneratorManager);
        assertNotNull(actualScripts);
        Collection<String> actual = new ArrayList();
        for (Script script : actualScripts) {
            actual.add(script.getSQL());
        }
        assertEquals(newArrayList(expected), actual);
    }

    private Dialect createTargetDialect() {
        NuoDBDialect dialect = new NuoDBDialect();
        Database database = new Database();
        database.setDialect(dialect);
        return dialect;
    }
}
