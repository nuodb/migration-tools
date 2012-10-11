package com.nuodb.tools.migration.i18n;


import junit.framework.Assert;
import org.junit.Test;

public class ResourcesTest {

    public static final String TEST_RESOURCE_NAME = "migration.cli.migration.group.name";
    public static final String TEST_BUNDLE_NAME = "com.nuodb.tools.migration.i18n.messages";



    @Test
    public void testCustomLocale() throws Exception {
        final Resources resources = new Resources(TEST_BUNDLE_NAME);
        Assert.assertNotNull(resources);
        final String messageTest = resources.getMessage(TEST_RESOURCE_NAME);
        Assert.assertNotNull(messageTest);
        Assert.assertFalse(messageTest.length() == 0);
    }

    @Test
    public void testDefaultLocale() throws Exception {
        final Resources resources = Resources.getResources();
        Assert.assertNotNull(resources);
        final String messageTest = resources.getMessage(TEST_RESOURCE_NAME);
        Assert.assertNotNull(messageTest);
        Assert.assertFalse(messageTest.length() == 0);
    }
}
