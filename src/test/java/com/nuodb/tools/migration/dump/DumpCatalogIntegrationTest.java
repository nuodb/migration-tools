package com.nuodb.tools.migration.dump;


import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class DumpCatalogIntegrationTest {

    private static final String TEST_PATH = "/tmp/migration-tool-test";
    DumpCatalog catalog;

    @Before
    public void setUp() throws Exception {
        catalog = new DumpCatalog(TEST_PATH);
    }

    @Test
    public void testName() throws Exception {
        //To change body of created methods use File | Settings | File Templates.
    }

    @After
    public void tearDown() throws Exception {
        catalog.close();
        FileUtils.forceDelete(new File(TEST_PATH));
    }
}
