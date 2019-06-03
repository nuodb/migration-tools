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
package com.nuodb.migrator.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Character.*;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

/**
 * @author Sergey Bushik
 */
public class StringUtils {

    public static boolean isEmpty(CharSequence source) {
        return source == null || source.length() == 0;
    }

    public static boolean isLowerCase(CharSequence source) {
        if (isEmpty(source)) {
            return false;
        }
        int sz = source.length();
        for (int i = 0; i < sz; i++) {
            char ch = source.charAt(i);
            if (!(!Character.isLetter(ch) || Character.isLowerCase(ch))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isUpperCase(CharSequence source) {
        if (isEmpty(source)) {
            return false;
        }
        int sz = source.length();
        for (int i = 0; i < sz; i++) {
            char ch = source.charAt(i);
            if (!(!Character.isLetter(ch) || Character.isUpperCase(ch))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isCapitalizedCase(CharSequence source, char... delimiters) {
        return !isEmpty(source) && equals(capitalizedCase(source.toString(), delimiters), source);
    }

    public static boolean equals(CharSequence source1, CharSequence source2) {
        return source1 == null ? source2 == null : source1.equals(source2);
    }

    public static boolean equalsIgnoreCase(CharSequence source1, CharSequence source2) {
        boolean equalsIgnoreCase;
        if (source1 == null) {
            equalsIgnoreCase = source2 == null;
        } else {
            String str1 = source1.toString();
            String str2 = source2.toString();
            equalsIgnoreCase = str1.regionMatches(true, 0, str2, 0, Math.max(str1.length(), str2.length()));
        }
        return equalsIgnoreCase;
    }

    public static boolean isDelimiter(char ch, char[] delimiters) {
        if (delimiters == null) {
            return Character.isWhitespace(ch);
        }
        for (char delimiter : delimiters) {
            if (ch == delimiter) {
                return true;
            }
        }
        return false;
    }

    public static String lowerCase(CharSequence source) {
        return source == null ? null : source.toString().toLowerCase();
    }

    public static String upperCase(CharSequence source) {
        return source == null ? null : source.toString().toUpperCase();
    }

    public static String capitalizedCase(CharSequence source) {
        return capitalizedCase(source, null);
    }

    public static String capitalizedCase(CharSequence source, char... delimiters) {
        int delimitersCount = delimiters == null ? -1 : delimiters.length;
        if (isEmpty(source) || delimitersCount == 0) {
            return source.toString();
        }
        char[] buffer = source.toString().toLowerCase().toCharArray();
        boolean capitalizeNext = true;
        for (int i = 0; i < buffer.length; i++) {
            char ch = buffer[i];
            if (isDelimiter(ch, delimiters)) {
                capitalizeNext = true;
            } else if (capitalizeNext) {
                buffer[i] = toTitleCase(ch);
                capitalizeNext = false;
            }
        }
        return new String(buffer);
    }

    /**
     * Prepends prefix to the provided source, adjusts prefix case to the case
     * of the source.
     *
     * @param prefix
     *            to prepend to source
     * @param source
     *            to be prefixed
     * @param delimiter
     *            for capitalized case
     * @return auto cased string
     */
    public static String autoCase(CharSequence prefix, CharSequence source, char delimiter) {
        StringBuilder buffer = new StringBuilder();
        if (isLowerCase(source)) {
            buffer.append(lowerCase(prefix));
        } else if (isUpperCase(source)) {
            buffer.append(upperCase(prefix));
        } else if (isCapitalizedCase(source, delimiter)) {
            buffer.append(capitalizedCase(prefix, delimiter));
        } else {
            buffer.append(prefix);
        }
        buffer.append(delimiter);
        buffer.append(source);
        return buffer.toString();
    }

    public static int indexOf(String source, String token, int from) {
        return indexOf(source, token, from, false);
    }

    public static int indexOf(String source, String token, int from, boolean start) {
        return indexOf(source, token, from, start, false);
    }

    public static int indexOf(String source, String token, int from, boolean start, boolean end) {
        String regex = (start ? "\\s*" : "\\s+") + "(" + Pattern.quote(token) + ")" + (end ? "\\s*" : "\\s+");
        return indexOf(source, from, regex, CASE_INSENSITIVE, 1);
    }

    public static int indexOf(String source, int from, String regex, int flags, int group) {
        int index = -1, depth = 0, current = from;
        Matcher matcher = compile(regex, flags).matcher(source);
        while (current >= 0 && current < source.length()) {
            if (matcher.find(current)) {
                index = matcher.start(group);
                for (int i = current; i < index; i++) {
                    char c = source.charAt(i);
                    if (c == '(') {
                        depth = depth + 1;
                    } else if (c == ')') {
                        depth = depth - 1;
                    }
                }
                current = index + matcher.end(group);
            } else {
                index = -1;
            }
            if (depth == 0 || index != -1) {
                break;
            }
        }
        return depth == 0 ? index : -1;
    }
}
