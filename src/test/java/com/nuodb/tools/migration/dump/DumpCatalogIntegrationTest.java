package com.nuodb.tools.migration.dump;


import com.nuodb.tools.migration.jdbc.query.SelectQuery;
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

    @Before
    public void setUp() throws Exception {
        catalog = new DumpCatalog(TEST_PATH);
        Assert.assertEquals(catalog.getPath(), TEST_PATH);
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
        final SelectQuery query = createTestSelectQuery();
        final String entryName = catalog.getEntryName(query, "");
        Assert.assertTrue(entryName.startsWith("table-" + TEST_TABLE_NAME));
        Assert.assertTrue(entryName.endsWith("."));
    }

    @Test
    public void testAddEntry() throws Exception {
        catalog.open();
        final SelectQuery query = createTestSelectQuery();

        final OutputStream outputStream = catalog.addEntry(query, getDefaultOutputFormat().getType());
        outputStream.flush();
        Assert.assertNotNull(outputStream);
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
