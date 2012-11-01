package com.nuodb.tools.migration.output.catalog;


import com.nuodb.tools.migration.TestConstants;
import com.nuodb.tools.migration.output.format.csv.CsvDataFormat;
import com.nuodb.tools.migration.spec.FormatSpec;
import com.nuodb.tools.migration.spec.FormatSpecBase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class DumpCatalogIntegrationTest extends TestConstants {

    private static final String TEST_DIR = "/tmp/migration-tool";
    private static final String TEST_PATH = TEST_DIR + "/migration-tool-test";

    private EntryCatalogImpl catalog;
    private EntryWriterImpl writer;
    private FormatSpec outputSpec;

    @Before
    public void setUp() throws Exception {
        FormatSpec outputSpec = new FormatSpecBase();
        outputSpec.setPath(TEST_PATH);
        outputSpec.setType(CsvDataFormat.TYPE);
        this.outputSpec = outputSpec;

        EntryCatalogImpl catalog = new EntryCatalogImpl(outputSpec.getType());
        Assert.assertEquals(catalog.getPath(), TEST_PATH);
    }

    @Test
    public void testOpen() throws Exception {
        try {
            writer = (EntryWriterImpl) catalog.openWriter();
        } catch (EntryCatalogException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test(expected = EntryCatalogException.class)
    public void testOpenError() throws Exception {
        new EntryCatalogImpl("").openWriter();
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
