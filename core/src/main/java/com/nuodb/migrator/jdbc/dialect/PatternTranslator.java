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
package com.nuodb.migrator.jdbc.dialect;

import com.google.common.collect.Maps;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;

/**
 * @author Sergey Bushik
 */
public class PatternTranslator extends TranslatorBase {

    private final Map<Pattern, String> translations = Maps.newHashMap();

    public PatternTranslator(DatabaseInfo sourceDatabaseInfo) {
        super(sourceDatabaseInfo);
    }

    public PatternTranslator(DatabaseInfo sourceDatabaseInfo, DatabaseInfo targetDatabaseInfo) {
        super(sourceDatabaseInfo, targetDatabaseInfo);
    }

    public void addTranslation(String sourceScript, String targetScript) {
        addTranslationRegex("^(?i)" + quote(sourceScript) + "$", targetScript);
    }

    public void addTranslations(Collection<String> sourceScripts, String targetScript) {
        for (String sourceScript : sourceScripts) {
            addTranslation(sourceScript, targetScript);
        }
    }

    public void addTranslationRegex(String sourceScriptRegex, String targetScript) {
        addTranslationPattern(compile(sourceScriptRegex), targetScript);
    }

    public void addTranslationPattern(Pattern sourceScriptPattern, String targetScript) {
        translations.put(sourceScriptPattern, targetScript);
    }

    @Override
    public boolean supportsScript(Script script, TranslationContext context) {
        return script.getScript() != null;
    }

    @Override
    public Script translate(Script script, TranslationContext context) {
        for (Map.Entry<Pattern, String> translation : translations.entrySet()) {
            Matcher matcher = translation.getKey().matcher(script.getScript());
            if (matcher.find()) {
                return new SimpleScript(translate(matcher, translation.getValue()));
            }
        }
        return null;
    }

    protected String translate(Matcher matcher, String targetScript) {
        StringBuffer translation = new StringBuffer();
        do {
            matcher.appendReplacement(translation, targetScript);
        } while (matcher.find());
        matcher.appendTail(translation);
        return translation.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof PatternTranslator))
            return false;
        if (!super.equals(o))
            return false;

        PatternTranslator that = (PatternTranslator) o;

        if (translations != null ? !translations.equals(that.translations) : that.translations != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (translations != null ? translations.hashCode() : 0);
        return result;
    }
}
