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
package com.nuodb.migrator.cli.parse.option;

import java.util.Collections;
import java.util.Set;

/**
 * @author Sergey Bushik
 */
public class OptionFormat {

    public static final OptionFormat LONG = new OptionFormat("--", "=", ",");
    public static final OptionFormat SHORT = new OptionFormat("-", "=", ",");

    private Set<String> prefixes;
    private String argumentSeparator;
    private String valuesSeparator;

    public OptionFormat(OptionFormat optionFormat) {
        this(optionFormat.getPrefixes(), optionFormat.getArgumentSeparator(), optionFormat.getValuesSeparator());
    }

    public OptionFormat(String optionPrefix, String argumentSeparator, String valuesSeparator) {
        this(Collections.singleton(optionPrefix), argumentSeparator, valuesSeparator);
    }

    public OptionFormat(Set<String> optionPrefixes, String argumentSeparator, String valuesSeparator) {
        this.prefixes = optionPrefixes;
        this.argumentSeparator = argumentSeparator;
        this.valuesSeparator = valuesSeparator;
    }

    public Set<String> getPrefixes() {
        return prefixes;
    }

    public void setPrefixes(Set<String> prefixes) {
        this.prefixes = prefixes;
    }

    public String getArgumentSeparator() {
        return argumentSeparator;
    }

    public void setArgumentSeparator(String argumentSeparator) {
        this.argumentSeparator = argumentSeparator;
    }

    public String getValuesSeparator() {
        return valuesSeparator;
    }

    public void setValuesSeparator(String valuesSeparator) {
        this.valuesSeparator = valuesSeparator;
    }
}
