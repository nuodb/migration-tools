package com.nuodb.migration.resultset.catalog;


import com.nuodb.migration.TestUtils;
import com.nuodb.migration.resultset.format.csv.CsvAttributes;
import com.nuodb.migration.spec.ResourceSpec;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class FileCatalogIntegrationTest extends TestUtils {

    private static final String DIR = "/tmp/nuodb-migration";

    private CatalogWriter writer;
    private FileCatalog fileCatalog;

    @Before
    public void setUp() throws Exception {
        ResourceSpec outputSpec = new ResourceSpec();
        outputSpec.setPath(DIR);
        outputSpec.setType(CsvAttributes.FORMAT);

        FileCatalog catalogFile = new FileCatalog(outputSpec.getPath());
        Assert.assertEquals(DIR, catalogFile.getPath());
        this.fileCatalog = catalogFile;
    }

    @Test
    public void testOpen() throws Exception {
        try {
            writer = fileCatalog.getCatalogWriter();
        } catch (CatalogException exception) {
            Assert.fail(exception.getMessage());
        }
    }

    @After
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        File file = fileCatalog.getCatalogDir();
        if (file != null && file.exists()) {
            FileUtils.forceDelete(file);
        }
    }
}
