package com.nuodb.tools.migration.match;

public interface PatternCompiler {

    public Matcher matcher(String pattern);
}
