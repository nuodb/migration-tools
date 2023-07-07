package com.nuodb.migrator.backup.globalStore;

import com.nuodb.migrator.backup.loader.BackupLoader;
import com.nuodb.migrator.backup.loader.LoadTable;
import com.nuodb.migrator.dump.DumpJob;
import com.nuodb.migrator.globalStore.GlobalStore;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.Script;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;

import com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createTable;
import static java.sql.Types.BIGINT;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createSequence;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static org.mockito.BDDMockito.*;
import static org.mockito.Mockito.when;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.NUODB;
import static org.testng.Assert.*;

import java.sql.DatabaseMetaData;
import java.util.Collection;
import java.util.Map;

import static org.mockito.Mockito.mock;

import com.nuodb.migrator.spec.DumpJobSpec;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.mockito.MockitoAnnotations.initMocks;

import org.mockito.Spy;
import com.nuodb.migrator.job.JobExecution;
import com.nuodb.migrator.job.JobExecutor;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.job.JobExecutors.createJobExecutor;


import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;


//@SuppressWarnings("all")
public class GlobalStoreTest {

    private GlobalStore globalStore;
    private Table table;
    @Mock
    public LoadTable mockedLoadTable;
    private ScriptGeneratorManager scriptGeneratorManager;
    private BackupLoader backupLoader;
    @Spy
    @InjectMocks
    private DumpJob dumpJob = new DumpJob(new DumpJobSpec());
    private JobExecutor jobExecutor;
    private Map<Object, Object> jobContext;


    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        GlobalStore globalStore = GlobalStore.getInstance();
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        given(databaseMetaData.getDatabaseProductName()).willReturn("NuoDB");


    }


    @DataProvider(name = "alterScript")
    public Object[][] testAlterScriptWithGeneratedAlwaysTrue() {
        GlobalStore gb = GlobalStore.getInstance();


        Dialect dialect = new NuoDBDialect();
        Schema schema = new Schema("schema1");
        Table table = new Table("test_table");
        Column column = table.addColumn("id");
        column.setTypeCode(BIGINT);
        column.setTypeName("BIGINT");
        column.setNullable(false);
        column.setPosition(1);
        Sequence sequence = createSequence(null, "schema1", "test_table", "id");
        sequence.setName("sequence");
        column.setSequence(sequence);
        schema.addSequence(sequence);
        column.getSequence().setGeneratedAlways(true);
        table.addColumn(column);
        schema.addTable(table);
        gb.put(sequence.getName(), sequence.isGeneratedAlways());
        String alterStat = "ALTER TABLE " + schema.getName() + "." + table.getName() + " MODIFY " + column.getName() + " " + column.getJdbcType().getTypeName() + " GENERATED ALWAYS AS IDENTITY";


        return new Object[][]{
                {table, gb, dialect, column, alterStat}
        };
    }

    @Test(dataProvider = "alterScript")
    public void testGeneratedAlwaysTrue(Table table, GlobalStore gb, Dialect dialect, Column col, String expected) {

        LoadTable tb = new LoadTable(null, table, null);

        String actualScripts = gb.alterScript(tb);

        assertEquals(expected, actualScripts);


    }

    public Object[][] testAlterScriptWithGeneratedAlwaysFalse() {
        GlobalStore gb = GlobalStore.getInstance();
        Dialect dialect = new NuoDBDialect();
        Schema schema2 = new Schema("schema2");
        Table table2 = new Table("test_table2");
        Column column2 = table2.addColumn("id2");
        column2.setTypeCode(BIGINT);
        column2.setTypeName("BIGINT");
        column2.setNullable(false);
        column2.setPosition(1);
        Sequence sequence2 = createSequence(null, "schema2", "test_table2", "id2");
        sequence2.setName("sequence2");
        column2.setSequence(sequence2);
        schema2.addSequence(sequence2);
        column2.getSequence().setGeneratedAlways(false);
        table2.addColumn(column2);
        schema2.addTable(table2);
        gb.put(sequence2.getName(), sequence2.isGeneratedAlways());

        return new Object[][]{{table2, gb, dialect, column2, null}};

    }

    @Test(dataProvider = "alterScript")
    public void testGeneratedAlwaysFalse(Table table, GlobalStore gb, Dialect dialect, Column col, String expected) {

        LoadTable tb = new LoadTable(null, table, null);

        String actualScripts = gb.alterScript(tb);

        assertEquals(expected, actualScripts);


    }

    public Object[][] testAlterScriptWithoutGeneratedAlways() {
        GlobalStore gb = GlobalStore.getInstance();
        Dialect dialect = new NuoDBDialect();
        Schema schema3 = new Schema("schema3");
        Table table3 = new Table("test_table3");
        Column column3 = table3.addColumn("id3");
        column3.setTypeCode(BIGINT);
        column3.setTypeName("BIGINT");
        column3.setNullable(false);
        column3.setPosition(1);
        Sequence sequence3 = createSequence(null, "schema3", "test_table3", "id3");
        sequence3.setName("sequence3");
        column3.setSequence(sequence3);
        schema3.addSequence(sequence3);
        table3.addColumn(column3);
        schema3.addTable(table3);
        gb.put(sequence3.getName(), sequence3.isGeneratedAlways());
        String alterStat3 = "";

        return new Object[][] {  {table3, gb, dialect, column3, null}};
    }

    @Test(dataProvider = "alterScript")
    public void testWithoutGeneratedAlways(Table table, GlobalStore gb, Dialect dialect, Column col, String expected) {

        LoadTable tb = new LoadTable(null, table, null);

        String actualScripts = gb.alterScript(tb);

        assertEquals(expected, actualScripts);


    }

}
