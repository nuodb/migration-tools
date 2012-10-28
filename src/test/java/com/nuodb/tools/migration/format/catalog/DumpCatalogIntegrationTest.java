package com.nuodb.tools.migration.format.catalog;


import com.nuodb.tools.migration.TestConstants;
import com.nuodb.tools.migration.format.CsvFormat;
import com.nuodb.tools.migration.format.catalog.entry.SelectQueryEntry;
import com.nuodb.tools.migration.jdbc.query.SelectQuery;
import com.nuodb.tools.migration.spec.FormatSpec;
import com.nuodb.tools.migration.spec.FormatSpecBase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.OutputStream;

public class DumpCatalogIntegrationTest extends TestConstants {

    private static final String TEST_DIR = "/tmp/migration-tool";
    private static final String TEST_PATH = TEST_DIR + "/migration-tool-test";

    private QueryEntryCatalogImpl catalog;
    private QueryEntryWriterImpl writer;
    private FormatSpec outputSpec;

    @Before
    public void setUp() throws Exception {
        FormatSpec outputSpec = new FormatSpecBase();
        outputSpec.setPath(TEST_PATH);
        outputSpec.setType(CsvFormat.TYPE);
        this.outputSpec = outputSpec;

        QueryEntryCatalogImpl catalog = new QueryEntryCatalogImpl(outputSpec.getType(), outputSpec.getType());
        Assert.assertEquals(catalog.getPath(), TEST_PATH);
    }

    @Test
    public void testOpen() throws Exception {
        try {
            writer = (QueryEntryWriterImpl) catalog.openQueryEntryWriter();
        } catch (QueryEntryCatalogException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetEntryName() throws Exception {
        final SelectQuery query = createTestSelectQuery();
        final SelectQueryEntry entry = new SelectQueryEntry(query);
        Assert.assertTrue(entry.getName().equals("table-" + TEST_TABLE_NAME));
    }

    @Test
    public void testAddEntry() throws Exception {
        writer.open();
        final SelectQuery query = createTestSelectQuery();

        final OutputStream outputStream = writer.write(new SelectQueryEntry(query));
        outputStream.flush();
        Assert.assertNotNull(outputStream);
    }

    @Test(expected = QueryEntryCatalogException.class)
    public void testOpenError() throws Exception {
        new QueryEntryCatalogImpl("", outputSpec.getType()).openQueryEntryWriter();
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
