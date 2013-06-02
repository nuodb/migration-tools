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
package com.nuodb.migrator.jdbc.dialect;

import com.google.common.collect.Maps;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

/**
 * @author Sergey Bushik
 */
public class PatternScriptTranslator extends ScriptTranslatorBase {

    private final Map<Pattern, String> scriptTranslations = Maps.newHashMap();

    public PatternScriptTranslator() {
    }

    public PatternScriptTranslator(DatabaseInfo sourceDatabaseInfo) {
        super(sourceDatabaseInfo);
    }

    public PatternScriptTranslator(DatabaseInfo sourceDatabaseInfo, DatabaseInfo targetDatabaseInfo) {
        super(sourceDatabaseInfo, targetDatabaseInfo);
    }

    public void addScriptTranslation(String sourceScript, String targetScript) {
        addScriptTranslationRegex("^" + Pattern.quote(sourceScript) + "$", targetScript);
    }

    public void addScriptTranslations(Collection<String> sourceScripts, String targetScript) {
        for (String sourceScript : sourceScripts) {
            addScriptTranslation(sourceScript, targetScript);
        }
    }

    public void addScriptTranslationRegex(String sourceScriptRegex, String targetScript) {
        addScriptTranslationRegex(sourceScriptRegex, CASE_INSENSITIVE, targetScript);
    }

    public void addScriptTranslationRegex(String sourceScriptRegex, int flags, String targetScript) {
        addScriptTranslationPattern(compile(sourceScriptRegex, flags), targetScript);
    }

    public void addScriptTranslationPattern(Pattern sourceScriptPattern, String targetScript) {
        scriptTranslations.put(sourceScriptPattern, targetScript);
    }

    @Override
    protected String getScriptTranslation(String sourceScript, DatabaseInfo sourceDatabaseInfo,
                                          DatabaseInfo targetDatabaseInfo) {
        for (Map.Entry<Pattern, String> scriptTranslation : scriptTranslations.entrySet()) {
            Matcher matcher = scriptTranslation.getKey().matcher(sourceScript);
            if (matcher.find()) {
                return getScriptTranslation(matcher, scriptTranslation.getValue());
            }
        }
        return null;
    }

    protected String getScriptTranslation(Matcher matcher, String targetScript) {
        StringBuffer translation = new StringBuffer();
        do {
            matcher.appendReplacement(translation, targetScript);
        } while (matcher.find());
        matcher.appendTail(translation);
        return translation.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PatternScriptTranslator)) return false;
        if (!super.equals(o)) return false;

        PatternScriptTranslator that = (PatternScriptTranslator) o;

        if (scriptTranslations != null ? !scriptTranslations.equals(
                that.scriptTranslations) : that.scriptTranslations != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (scriptTranslations != null ? scriptTranslations.hashCode() : 0);
        return result;
    }
}
