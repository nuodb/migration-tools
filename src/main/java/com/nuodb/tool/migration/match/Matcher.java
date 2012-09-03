package com.nuodb.tool.migration.match;

public interface Matcher {

    String pattern();

    boolean matches(String value);
}
