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

import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.ColumnTrigger;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.*;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class NamingStrategyTest {

    private ScriptGeneratorManager scriptGeneratorManager;
    private String catalog;
    private String schema;

    @BeforeMethod
    public void setUp() {
        NuoDBDialect dialect = new NuoDBDialect();

        Database database = new Database();
        database.setDialect(dialect);

        scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.setTargetDialect(dialect);
        scriptGeneratorManager.setTargetCatalog(catalog = null);
        scriptGeneratorManager.setTargetSchema(schema = "s1");
    }

    @DataProvider(name = "scriptGeneratorManager.getName")
    public Object[][] createScriptGeneratorManagerGetNameData() {
        return new Object[][] { { createSchema(null, "s2"), false, "s2" }, { createSchema(null, "s2"), true, "\"s2\"" },
                { createTable(null, "s2", "t1"), false, "t1" }, { createTable(null, "s2", "t1"), true, "\"t1\"" },
                { createColumn(null, "s2", "t1", "c1"), false, "c1" },
                { createColumn(null, "s2", "t1", "c1"), true, "\"c1\"" } };
    }

    @DataProvider(name = "scriptGeneratorManager.getQualifiedName")
    public Object[][] createScriptGeneratorManagerGetQualifiedNameData() {
        return new Object[][] { { createSchema(null, "s2"), false, "s2" }, { createSchema(null, "s2"), true, "\"s2\"" },
                { createTable(null, "s2", "t1"), false, "s1.t1" },
                { createTable(null, "s2", "t1"), true, "\"s1\".\"t1\"" },
                { createColumn(null, "s2", "t1", "c1"), false, "c1" },
                { createColumn(null, "s2", "t1", "c1"), true, "\"c1\"" } };
    }

    @Test(dataProvider = "scriptGeneratorManager.getName")
    public void testScriptGeneratorManagerGetName(MetaData object, boolean normalize, String name) {
        assertEquals(scriptGeneratorManager.getName(object, normalize), name);
    }

    @Test(dataProvider = "scriptGeneratorManager.getQualifiedName")
    public void testScriptGeneratorManagerGetQualifiedName(MetaData object, boolean normalize, String qualifiedName) {
        assertEquals(scriptGeneratorManager.getQualifiedName(object, normalize), qualifiedName);
    }

    @DataProvider(name = "namingStrategy.getQualifiedName")
    public Object[][] createNamingStrategyGetQualifiedNameData() {
        Table t1 = createTable(null, "s2", "t1");
        Table t2 = createTable(null, "s3", "t2");

        ForeignKey foreignKey = createForeignKey("fk1",
                asList(t1.addColumn("c1_1234567890_1234567890_1234567890_1234567890_1234567890")),
                asList(t2.addColumn("c2_1234567890_1234567890_1234567890_1234567890_1234567890")));

        Index index = createIndex("idx1", t1.getColumns(), true);
        Sequence sequence = createSequence("seq1", null, "s3", "t1",
                "c1_1234567890_1234567890_1234567890_1234567890_1234567890");

        ColumnTrigger trigger = new ColumnTrigger("trg1");
        Column column = get(t1.getColumns(), 0);
        trigger.setColumn(column);
        column.getTable().addTrigger(trigger);
        return new Object[][] { { new ForeignKeyQualifyNamingStrategy(), foreignKey, false,
                "fk_s1.t1_c1_1234567890_1234567890_1234567890_1234567890_1234567890_s1.t2_c2_1234567890_1234567890_1234567890_1234567890_1234567890" },
                { new ForeignKeyHashNamingStrategy(), foreignKey, false, "fk_877e7d95" },
                { new ForeignKeyAutoNamingStrategy(), foreignKey, false, "fk_t2_fk1" },
                { new ForeignKeySourceNamingStrategy(), foreignKey, false, "fk_t2_fk1" },
                { new IndexQualifyNamingStrategy(), index, false,
                        "idx_unique_t1_c1_1234567890_1234567890_1234567890_1234567890_1234567890" },
                { new IndexHashNamingStrategy(), index, false, "idx_ccd246f8" },
                { new IndexAutoNamingStrategy(), index, false, "idx_t1_idx1" },
                { new IndexSourceNamingStrategy(), index, false, "idx_t1_idx1" },
                { new SequenceQualifyNamingStrategy(), sequence, false,
                        "s1.seq_t1_c1_1234567890_1234567890_1234567890_1234567890_1234567890" },
                { new SequenceHashNamingStrategy(), sequence, false, "s1.seq_11bd414a" },
                { new SequenceAutoNamingStrategy(), sequence, false, "s1.t1_seq1" },
                { new SequenceSourceNamingStrategy(), sequence, false, "s1.t1_seq1" },
                { new TriggerQualifyNamingStrategy(), trigger, false, "trg_t1_0" },
                { new TriggerHashNamingStrategy(), trigger, false, "trg_357eae" },
                { new TriggerAutoNamingStrategy(), trigger, false, "t1_trg1" },
                { new TriggerSourceNamingStrategy(), trigger, false, "t1_trg1" }, };
    }

    @Test(dataProvider = "namingStrategy.getQualifiedName")
    public <T extends MetaData> void testNamingStrategyGetQualifiedName(NamingStrategy<T> namingStrategy, T object,
            boolean normalize, String name) {
        assertEquals(namingStrategy.getQualifiedName(object, scriptGeneratorManager, catalog, schema, normalize), name);
    }
}
