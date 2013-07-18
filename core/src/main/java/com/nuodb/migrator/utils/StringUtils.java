/**
 * Copyright (c) 2012, NuoDB, Inc.
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

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

/**
 * @author Sergey Bushik
 */
public class StringUtils {

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
