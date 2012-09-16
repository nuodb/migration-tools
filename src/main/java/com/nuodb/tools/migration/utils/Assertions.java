package com.nuodb.tools.migration.utils;

public class Assertions {

    private Assertions() {
    }

    public static void assertNotNull(Object object) {
        assertNotNull(object, "Object should be not null");
    }

    public static void assertNotNull(Object object, String message) {
        if (object == null) {
            fail(message);
        }
    }

    public static void assertTrue(boolean expression) {
        assertTrue(expression, "True value expected");
    }

    public static void assertTrue(boolean expression, String message) {
        if (!expression) {
            fail(message);
        }
    }

    public static void fail(String message) {
        throw new AssertionException(message);
    }
}
