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
package com.nuodb.migrator.config;

import java.util.Properties;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class Replacer {

    public static final String PLACEHOLDER_PREFIX = "${";

    public static final String PLACEHOLDER_SUFFIX = "}";

    private String placeholderPrefix = PLACEHOLDER_PREFIX;

    private String placeholderSuffix = PLACEHOLDER_SUFFIX;

    private boolean ignoreUnknownPlaceholders;

    private Properties replacements = new Properties();

    public Replacer() {
    }

    public Replacer(Properties replacements) {
        addReplacements(replacements);
    }

    public String replace(String property) {
        if (property == null) {
            return null;
        }
        StringBuilder buffer = new StringBuilder(property);
        int prefixStart = property.indexOf(placeholderPrefix);
        while (prefixStart != -1) {
            int replacementStart = prefixStart + placeholderPrefix.length();
            int suffixStart = buffer.indexOf(placeholderSuffix, replacementStart);
            if (suffixStart != -1) {
                String placeholder = buffer.substring(replacementStart, suffixStart);
                String replacement = getReplacement(placeholder);
                if (replacement == null) {
                    if (isIgnoreUnknownPlaceholders()) {
                        prefixStart = buffer.indexOf(placeholderPrefix, suffixStart);
                    } else {
                        throw new IllegalArgumentException(
                                format("Can't find replacement for %s placeholder", placeholder));
                    }
                } else {
                    buffer.replace(prefixStart, suffixStart + placeholderSuffix.length(), replacement);
                    prefixStart = buffer.indexOf(placeholderPrefix, prefixStart + replacement.length());
                }
            } else {
                prefixStart = -1;
            }
        }
        return buffer.toString();
    }

    public String getReplacement(String placeholder) {
        return replacements.getProperty(placeholder);
    }

    public void addReplacements(Properties replacements) {
        this.replacements.putAll(replacements);
    }

    public String getPlaceholderPrefix() {
        return placeholderPrefix;
    }

    public void setPlaceholderPrefix(String placeholderPrefix) {
        this.placeholderPrefix = placeholderPrefix;
    }

    public String getPlaceholderSuffix() {
        return placeholderSuffix;
    }

    public void setPlaceholderSuffix(String placeholderSuffix) {
        this.placeholderSuffix = placeholderSuffix;
    }

    public boolean isIgnoreUnknownPlaceholders() {
        return ignoreUnknownPlaceholders;
    }

    public void setIgnoreUnknownPlaceholders(boolean ignoreUnknownPlaceholders) {
        this.ignoreUnknownPlaceholders = ignoreUnknownPlaceholders;
    }
}