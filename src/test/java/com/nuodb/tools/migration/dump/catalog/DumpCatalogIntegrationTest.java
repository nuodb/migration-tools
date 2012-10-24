package com.nuodb.tools.migration.dump.catalog;


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

    private CatalogImpl catalog;
    private CatalogWriterImpl writer;

    @Before
    public void setUp() throws Exception {
        catalog = new CatalogImpl(TEST_PATH);
        Assert.assertEquals(catalog.getPath(), TEST_PATH);
    }

    @Test
    public void testOpen() throws Exception {
        try {
            writer = (CatalogWriterImpl) catalog.openWriter();
        } catch (CatalogException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetEntryName() throws Exception {
        final SelectQuery query = createTestSelectQuery();
        final String entryName = writer.getEntryName(query, "");
        Assert.assertTrue(entryName.startsWith("table-" + TEST_TABLE_NAME));
        Assert.assertTrue(entryName.endsWith("."));
    }

    @Test
    public void testAddEntry() throws Exception {
        writer.open();
        final SelectQuery query = createTestSelectQuery();

        final OutputStream outputStream = writer.openEntry(query, getDefaultOutputFormat().getType());
        outputStream.flush();
        Assert.assertNotNull(outputStream);
    }

    @Test(expected = CatalogException.class)
    public void testOpenError() throws Exception {
        new CatalogImpl("").openWriter();
    }

    @After
    public void tearDown() throws Exception {
        writer.close();
        final File file = catalog.getCatalogDir();
        if (file != null && file.exists()) {
            FileUtils.forceDelete(file);
        }
    }
}
