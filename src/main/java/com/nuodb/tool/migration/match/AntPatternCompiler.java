package com.nuodb.tool.migration.match;

import java.util.regex.Pattern;

/**
 * Compiles Ant style pattern to a matcher, where each '*' wildcard character interpreted as any number of symbols and
 * ? sign meaning any single character.
 *
 * @author Sergey Bushik
 */
public class AntPatternCompiler implements PatternCompiler {

    private static final Pattern ANT_PATTERN = Pattern.compile("\\?|\\*|\\{([^/]+?)\\}");

    private static final String EMPTY = "";

    public Matcher matcher(String pattern) {
        return new RegexMatcher(pattern, compile(pattern));
    }

    public static Pattern compile(String pattern) {
        if (pattern == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        java.util.regex.Matcher matcher = ANT_PATTERN.matcher(pattern);
        int end = 0;
        while (matcher.find()) {
            builder.append(quote(pattern, end, matcher.start()));
            String match = matcher.group();
            if ("?".equals(match)) {
                builder.append('.');
            } else if ("*".equals(match)) {
                builder.append(".*");
            }
            end = matcher.end();
        }
        builder.append(quote(pattern, end, pattern.length()));
        return Pattern.compile(builder.toString());
    }

    public static String quote(String pattern, int start, int end) {
        return start == end ? EMPTY : Pattern.quote(pattern.substring(start, end));
    }
}
