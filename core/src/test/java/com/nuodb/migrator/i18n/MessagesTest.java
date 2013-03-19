package com.nuodb.migrator.i18n;

import com.nuodb.migrator.context.SimpleMessages;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class MessagesTest {

    public static final String RESOURCE_NAME = "com.nuodb.migrator.root.group.name";
    public static final String BUNDLE_NAME = "com.nuodb.migrator.i18n.messages";

    @Test
    public void testWithCustomLocale() throws Exception {
        Messages messages = new SimpleMessages(BUNDLE_NAME);
        assertNotNull(messages);
        String message = messages.getMessage(RESOURCE_NAME);
        assertNotNull(message);
        assertFalse(message.length() == 0);
    }

    @Test
    public void testWithDefaultLocale() throws Exception {
        Messages messages = SimpleMessages.getInstance();
        assertNotNull(messages);
        String message = messages.getMessage(RESOURCE_NAME);
        assertNotNull(message);
        assertFalse(message.length() == 0);
    }
}
