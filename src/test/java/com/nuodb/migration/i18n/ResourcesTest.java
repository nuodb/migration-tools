package com.nuodb.migration.i18n;


import junit.framework.Assert;
import org.junit.Test;

public class ResourcesTest {

    public static final String RESOURCE_NAME = "com.nuodb.migration.cli.migration.group.name";
    public static final String BUNDLE_NAME = "com.nuodb.migration.i18n.messages";

    @Test
    public void testCustomLocale() throws Exception {
        final Resources resources = new Resources(BUNDLE_NAME);
        Assert.assertNotNull(resources);
        final String message = resources.getMessage(RESOURCE_NAME);
        Assert.assertNotNull(message);
        Assert.assertFalse(message.length() == 0);
    }

    @Test
    public void testDefaultLocale() throws Exception {
        final Resources resources = Resources.getResources();
        Assert.assertNotNull(resources);
        final String message = resources.getMessage(RESOURCE_NAME);
        Assert.assertNotNull(message);
        Assert.assertFalse(message.length() == 0);
    }
}
