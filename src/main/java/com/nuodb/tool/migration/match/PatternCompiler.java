package com.nuodb.tool.migration.match;

public interface PatternCompiler {

    public Matcher matcher(String pattern);
}
