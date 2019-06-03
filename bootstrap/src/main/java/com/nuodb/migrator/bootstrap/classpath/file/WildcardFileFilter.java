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
package com.nuodb.migrator.bootstrap.classpath.file;

import java.io.File;
import java.io.FileFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

/**
 * @author Sergey Bushik
 */
public class WildcardFileFilter implements FileFilter {

    public static final Pattern WILDCARDS = compile("\\?|\\*|\\{([^/]+?)\\}", CASE_INSENSITIVE);
    private static final String EMPTY = "";

    private final String name;
    private final Pattern pattern;

    public WildcardFileFilter(String name) {
        this.name = name;
        this.pattern = createPattern(name);
    }

    @Override
    public boolean accept(File file) {
        return pattern.matcher(file.getName()).matches();
    }

    private static Pattern createPattern(String path) {
        StringBuilder builder = new StringBuilder();
        Matcher matcher = WILDCARDS.matcher(path);
        int end = 0;
        while (matcher.find()) {
            builder.append(quote(path, end, matcher.start()));
            String match = matcher.group();
            if ("?".equals(match)) {
                builder.append(".");
            } else if ("*".equals(match)) {
                builder.append(".*");
            } else {
                builder.append(matcher.group());
            }
            end = matcher.end();
        }
        builder.append(quote(path, end, path.length()));
        return compile(builder.toString());
    }

    private static String quote(String pattern, int start, int end) {
        return start == end ? EMPTY : Pattern.quote(pattern.substring(start, end));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof WildcardFileFilter))
            return false;

        WildcardFileFilter that = (WildcardFileFilter) o;

        if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return pattern != null ? pattern.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "WildcardFileFilter{name=" + name + '}';
    }
}
