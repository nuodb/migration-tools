package com.nuodb.migrator.i18n;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class ResourcesTest {

    public static final String RESOURCE_NAME = "com.nuodb.migrator.root.group.name";
    public static final String BUNDLE_NAME = "com.nuodb.migrator.i18n.messages";

    @Test
    public void testCustomLocale() throws Exception {
        Resources resources = new Resources(BUNDLE_NAME);
        assertNotNull(resources);
        String message = resources.getMessage(RESOURCE_NAME);
        assertNotNull(message);
        assertFalse(message.length() == 0);
    }

    @Test
    public void testDefaultLocale() throws Exception {
        Resources resources = Resources.getResources();
        assertNotNull(resources);
        String message = resources.getMessage(RESOURCE_NAME);
        assertNotNull(message);
        assertFalse(message.length() == 0);
    }
}
