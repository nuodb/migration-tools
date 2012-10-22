package com.nuodb.tools.migration.dump;


import com.nuodb.tools.migration.TestConstants;
import com.nuodb.tools.migration.dump.output.OutputFormat;
import com.nuodb.tools.migration.dump.output.OutputFormatLookupImpl;
import com.nuodb.tools.migration.dump.query.SelectQuery;
import com.nuodb.tools.migration.jdbc.metamodel.Catalog;
import com.nuodb.tools.migration.jdbc.metamodel.Database;
import com.nuodb.tools.migration.jdbc.metamodel.Schema;
import com.nuodb.tools.migration.jdbc.metamodel.Table;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.io.File;
import java.io.OutputStream;

import static com.nuodb.tools.migration.TestConstants.*;

public class DumpCatalogIntegrationTest {

    private static final String TEST_DIR = "/tmp/migration-tool";
    private static final String TEST_PATH = TEST_DIR + "/migration-tool-test";



    DumpCatalog catalog;
    private OutputFormat outputFormat;

    @Before
    public void setUp() throws Exception {
        catalog = new DumpCatalog(TEST_PATH);
        Assert.assertEquals(catalog.getPath(), TEST_PATH);
        outputFormat = new OutputFormatLookupImpl().getDefaultFormatClass().newInstance();
    }

    @Test
    public void testOpen() throws Exception {
        catalog.open();
        Assert.assertNotNull(catalog.getCatalogDir());
        Assert.assertEquals(new File(TEST_DIR), catalog.getCatalogDir());
        Assert.assertEquals(new File(TEST_PATH), catalog.getCatalogFile());
    }


    @Test
    public void testGetEntryName() throws Exception {
        final SelectQuery query = createTestQuery();
        final String entryName = catalog.getEntryName(query, "");
        Assert.assertTrue(entryName.startsWith("table-" + TEST_TABLE_NAME));
        Assert.assertTrue(entryName.endsWith("."));
    }

    @Test
    public void testAddEntry() throws Exception {
        catalog.open();
        final SelectQuery query = createTestQuery();

        final OutputStream outputStream = catalog.addEntry(query, outputFormat.getType());
        outputStream.flush();
        Assert.assertNotNull(outputStream);
    }


    private SelectQuery createTestQuery() {
        final SelectQuery query = new SelectQuery();
        final Schema testSchema = new Schema(new Catalog(new Database(), TEST_CATALOG_NAME), TEST_SCHEMA_NAME);
        final Table testTable = new Table(testSchema, TEST_TABLE_NAME);
        testTable.createColumn(FIRST_COLUMN_NAME);
        testTable.createColumn(SECOND_COLUMN_NAME);
        query.addTable(testTable);
        return query;
    }


    @Test(expected = DumpException.class)
    public void testOpenError() throws Exception {
        new DumpCatalog("").open();
    }


    @After
    public void tearDown() throws Exception {
        catalog.close();
        final File file = catalog.getCatalogDir();
        if (file != null && file.exists()) {
            FileUtils.forceDelete(file);
        }
    }
}
