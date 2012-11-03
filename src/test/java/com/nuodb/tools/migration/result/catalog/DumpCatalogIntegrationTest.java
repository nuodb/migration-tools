package com.nuodb.tools.migration.result.catalog;


import com.nuodb.tools.migration.TestConstants;
import com.nuodb.tools.migration.result.format.csv.CsvAttributes;
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

    private ResultCatalogImpl catalogFile;
    private ResultEntryWriterImpl writer;
    private FormatSpec outputSpec;

    @Before
    public void setUp() throws Exception {
        FormatSpec outputSpec = new FormatSpecBase();
        outputSpec.setPath(TEST_PATH);
        outputSpec.setType(CsvAttributes.TYPE);
        this.outputSpec = outputSpec;

        ResultCatalogImpl catalogFile = new ResultCatalogImpl(outputSpec.getType());
        Assert.assertEquals(catalogFile.getPath(), TEST_PATH);
    }

    @Test
    public void testOpen() throws Exception {
        try {
            writer = (ResultEntryWriterImpl) catalogFile.openWriter();
        } catch (ResultCatalogException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test(expected = ResultCatalogException.class)
    public void testOpenError() throws Exception {
        new ResultCatalogImpl("").openWriter();
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
