package com.nuodb.tool.migration.match;

import java.util.regex.Pattern;

public class RegexMatcher implements Matcher {

    private String pattern;
    private Pattern regex;

    public RegexMatcher(String pattern, Pattern regex) {
        this.pattern = pattern;
        this.regex = regex;
    }

    public String pattern() {
        return pattern;
    }

    public boolean matches(String value) {
        return value == null ? regex == null : regex.matcher(value).matches();
    }
}
