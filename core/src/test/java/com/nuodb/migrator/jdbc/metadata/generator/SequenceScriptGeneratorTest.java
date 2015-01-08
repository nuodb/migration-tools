package com.nuodb.migrator.jdbc.metadata.generator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.POSTGRE_SQL;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.NUODB;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createSequence;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static java.sql.Types.INTEGER;

import java.sql.SQLException;
import java.util.Collection;

import junit.framework.Assert;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.nuodb.migrator.backup.loader.BackupLoader;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.dialect.PostgreSQLDialect;
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.utils.SequenceUtils;

public class SequenceScriptGeneratorTest {

    private ScriptGeneratorManager scriptGeneratorManager;
    private BackupLoader backupLoader;
    private HasSchemasScriptGenerator schemasScriptGenerator;

    @BeforeMethod
    public void setUp() throws SQLException {
        scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.setSourceSession(createSession(new PostgreSQLDialect(POSTGRE_SQL)));
        scriptGeneratorManager.setTargetDialect(new NuoDBDialect(NUODB));
        backupLoader = new BackupLoader();
        schemasScriptGenerator = new HasSchemasScriptGenerator();
    }

    @DataProvider(name = "getTableSeqScripts")
    public Object[][] getTableSeqScripts() {
        
        Database database1 = new Database();
        Catalog catalog1 = database1.addCatalog(valueOf("c1"));

        Table t1 = new Table("t1");
        Column c1 = t1.addColumn("id");
        c1.setTypeCode(INTEGER);
        c1.setTypeName("INTEGER");
        c1.setNullable(false);
        c1.setPosition(1);
        Sequence s1 = createSequence("seq1", null, "s3", "t1","c1");
        Sequence s2 = new Sequence(valueOf("seq2"));
        catalog1.addSchema(valueOf("s1")).addTable(t1);

        catalog1.addSchema(valueOf("s1")).addSequence(s1);
        catalog1.addSchema(valueOf("s1")).addSequence(s2);

        c1.setSequence(s1);
        t1.addColumn(c1);
        Collection<Sequence> allSequences1 = newArrayList();
        allSequences1.add(s1);
        allSequences1.add(s2);
        t1.setDatabase(database1);
        Collection<String> expected1 = newArrayList(
                "DROP SEQUENCE IF EXISTS \"seq2\"",
                "CREATE SEQUENCE \"seq2\""
        );

        Database database2 = new Database();
        Catalog catalog2 = database2.addCatalog(valueOf("c2"));
        Table t2 = new Table("t2");
        Column c2 = t2.addColumn("id");
        c2.setTypeCode(INTEGER);
        c2.setTypeName("INTEGER");
        c2.setNullable(false);
        c2.setPosition(1);
        Sequence s3 = createSequence("seq3", null, "s3", null,"id");
        catalog2.addSchema(valueOf("s2")).addTable(t2);
        catalog2.addSchema(valueOf("s2")).addSequence(s3);
        c2.setSequence(s3);
        t2.setDatabase(database2);
        t2.addColumn(c2);
        Collection<Sequence> allSequences2 = newArrayList();
        allSequences2.add(s3);
        Collection<String> expected2 = newArrayList();

        return new Object[][] {
                { database1.getCatalog("c1").getSchema("s1"), allSequences1, expected1 },
                { database2.getCatalog("c2").getSchema("s2"), allSequences2, expected2 }
                };
    }

    @Test(dataProvider = "getTableSeqScripts")
    public void testTableSeqScripts(Schema schema, Collection<Sequence> allSequences, Collection<String> expected) {
        Collection<Sequence>  sequences = SequenceUtils.getStandaloneSequences(schema, scriptGeneratorManager);
        Collection<String>  actual = backupLoader.getStandaloneSequenceScripts(sequences, scriptGeneratorManager);
        Assert.assertEquals(expected, actual);
    }

    @DataProvider(name = "getSchemaSeqScripts")
    public Object[][] getSchemaSeqScripts() {
        Schema schema1 = new Schema("schema1");
        Table t1 = new Table("t1");
        Column c1 = t1.addColumn("id");
        c1.setTypeCode(INTEGER);
        c1.setTypeName("INTEGER");
        c1.setNullable(false);
        c1.setPosition(1);
        Sequence s1 = createSequence("seq1", null, "schema1", "t1","c1");
        Sequence s2 = new Sequence(valueOf("seq2"));
        schema1.addSequence(s1);
        schema1.addSequence(s2);
        c1.setSequence(s1);
        t1.addColumn(c1);
        schema1.addTable(t1);
        Collection<String> expected1 = newArrayList(
                "DROP SEQUENCE IF EXISTS \"seq2\"",
                "CREATE SEQUENCE \"seq2\""
        );

        Schema schema2 = new Schema("schema2");
        Table t2 = new Table("t2");
        Column c2 = t2.addColumn("id");
        c2.setTypeCode(INTEGER);
        c2.setTypeName("INTEGER");
        c2.setNullable(false);
        c2.setPosition(1);
        Sequence s3 = new Sequence(valueOf("seq3"));
        Sequence s4 = new Sequence(valueOf("seq4"));
        schema2.addSequence(s3);
        schema2.addSequence(s4);
        t2.addColumn(c2);
        schema2.addTable(t2);
        Collection<String> expected2 = newArrayList(
                "DROP SEQUENCE IF EXISTS \"seq3\"",
                "CREATE SEQUENCE \"seq3\"",
                "DROP SEQUENCE IF EXISTS \"seq4\"",
                "CREATE SEQUENCE \"seq4\""
        );

        Schema schema3 = new Schema("schema3");
        Sequence s5 = createSequence("seq5", null, "schema2", null,null);
        Table t3 = new Table("t3");
        Column c3 = t3.addColumn("id");
        c3.setTypeCode(INTEGER);
        c3.setTypeName("INTEGER");
        c3.setNullable(false);
        c3.setPosition(1);
        c3.setSequence(s5);
        t3.addColumn(c3);
        schema3.addTable(t3);
        schema3.addSequence(s5);
        Collection<String> expected3 = newArrayList();

        return new Object[][]{
                {schema1, expected1},
                {schema2, expected2},
                {schema3, expected3}
                };
    }

    @Test(dataProvider = "getSchemaSeqScripts")
    public void testSchemaSeqScripts(Schema schema, Collection<String> expected) {
        Collection<Sequence>  sequences = SequenceUtils.getStandaloneSequences(schema, scriptGeneratorManager);
        Collection<String> actual = backupLoader.getStandaloneSequenceScripts(sequences, scriptGeneratorManager);
        Assert.assertEquals(expected, actual);
    }
}
