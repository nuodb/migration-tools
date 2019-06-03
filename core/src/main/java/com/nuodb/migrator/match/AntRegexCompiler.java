/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.match;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compiles Ant style pattern to a matcher, where each '*' wildcard character
 * interpreted as any number of symbols and '?' sign meaning any single
 * character. Builds additional capturing groups around wildcard symbols, so
 * that they are returned by {@link Match#matches()} method.
 *
 * @author Sergey Bushik
 */
public class AntRegexCompiler extends PatternCompilerBase {

    public static final AntRegexCompiler INSTANCE = new AntRegexCompiler();

    private static final String EMPTY = "";

    private final Pattern pattern;

    private AntRegexCompiler() {
        this(0);
    }

    public AntRegexCompiler(int flags) {
        pattern = Pattern.compile("\\?|\\*|\\{([^/]+?)\\}", flags);
    }

    public static boolean isPattern(String regex) {
        return INSTANCE.pattern.matcher(regex).find();
    }

    @Override
    protected Pattern pattern(String regex) {
        if (regex == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        Matcher matcher = pattern.matcher(regex);
        int end = 0;
        while (matcher.find()) {
            builder.append(quote(regex, end, matcher.start()));
            String match = matcher.group();
            if ("?".equals(match)) {
                builder.append("(.)");
            } else if ("*".equals(match)) {
                builder.append("(.*)");
            } else {
                builder.append(matcher.group());
            }
            end = matcher.end();
        }
        builder.append(quote(regex, end, regex.length()));
        return Pattern.compile(builder.toString());
    }

    protected String quote(String pattern, int start, int end) {
        return start == end ? EMPTY : Pattern.quote(pattern.substring(start, end));
    }
}