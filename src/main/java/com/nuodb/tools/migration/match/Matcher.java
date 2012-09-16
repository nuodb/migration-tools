package com.nuodb.tools.migration.match;

public interface Matcher {

    String pattern();

    boolean matches(String value);
}
