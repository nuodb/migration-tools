package com.nuodb.migration.resultset.catalog;


import com.nuodb.migration.TestConstants;
import com.nuodb.migration.resultset.format.csv.CsvAttributes;
import com.nuodb.migration.spec.FormatSpec;
import com.nuodb.migration.spec.FormatSpecBase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class DumpCatalogIntegrationTest extends TestConstants {

    private static final String TEST_DIR = "/tmp/migration-tool";
    private static final String TEST_PATH = TEST_DIR + "/migration-tool-test";

    private CatalogImpl catalogFile;
    private CatalogWriterImpl writer;
    private FormatSpec outputSpec;

    @Before
    public void setUp() throws Exception {
        FormatSpec outputSpec = new FormatSpecBase();
        outputSpec.setPath(TEST_PATH);
        outputSpec.setType(CsvAttributes.FORMAT_TYPE);
        this.outputSpec = outputSpec;

        CatalogImpl catalogFile = new CatalogImpl(outputSpec.getType());
        Assert.assertEquals(catalogFile.getPath(), TEST_PATH);
    }

    @Test
    public void testOpen() throws Exception {
        try {
            writer = (CatalogWriterImpl) catalogFile.getWriter();
        } catch (CatalogException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test(expected = CatalogException.class)
    public void testOpenError() throws Exception {
        new CatalogImpl("").getWriter();
    }

    @After
    public void tearDown() throws Exception {
        writer.close();
        final File file = catalogFile.getCatalogDir();
        if (file != null && file.exists()) {
            FileUtils.forceDelete(file);
        }
    }
}
