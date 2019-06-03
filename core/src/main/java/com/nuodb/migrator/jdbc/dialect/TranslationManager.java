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

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

import static com.nuodb.migrator.utils.Collections.newPrioritySet;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class TranslationManager {

    private TranslationConfig translationConfig = new TranslationConfig();
    private PrioritySet<Translator> translators = newPrioritySet();

    public TranslationConfig getTranslationConfig() {
        return translationConfig;
    }

    public void setTranslationConfig(TranslationConfig translationConfig) {
        this.translationConfig = translationConfig;
    }

    public Script translate(Script script, Dialect dialect, Session session, Map<Object, Object> context) {
        return translate(script, new SimpleTranslationContext(dialect, session, this, context));
    }

    public Script translate(Script script, TranslationContext context) {
        Script translation = null;
        for (Translator translator : getTranslators()) {
            if (translator.supports(script, context)) {
                translation = translator.translate(script, context);
            }
            if (translation != null) {
                break;
            }
        }
        return translation;
    }

    public void addTranslation(DatabaseInfo source, String sourceScript, DatabaseInfo target, String targetScript) {
        PatternTranslator translator = new PatternTranslator(source, target);
        translator.addTranslation(sourceScript, targetScript);
        addTranslator(translator);
    }

    public void addTranslations(DatabaseInfo source, Collection<String> sourceScripts, DatabaseInfo target,
            String targetScript) {
        PatternTranslator translator = new PatternTranslator(source, target);
        translator.addTranslations(sourceScripts, targetScript);
        addTranslator(translator);
    }

    public void addTranslationRegex(DatabaseInfo source, String sourceScript, DatabaseInfo target,
            String targetScript) {
        PatternTranslator translator = new PatternTranslator(source, target);
        translator.addTranslationRegex(sourceScript, targetScript);
        addTranslator(translator);
    }

    public void addTranslationPattern(DatabaseInfo source, Pattern sourceScript, DatabaseInfo target,
            String targetScript) {
        PatternTranslator translator = new PatternTranslator(source, target);
        translator.addTranslationPattern(sourceScript, targetScript);
        addTranslator(translator);
    }

    public void addTranslator(Translator translator) {
        translators.add(translator);
    }

    public void addTranslator(Translator translator, int priority) {
        translators.add(translator, priority);
    }

    public PrioritySet<Translator> getTranslators() {
        return translators;
    }

    public void setTranslators(PrioritySet<Translator> translators) {
        this.translators = translators;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof TranslationManager))
            return false;

        TranslationManager that = (TranslationManager) o;

        if (translators != null ? !translators.equals(that.translators) : that.translators != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return translators != null ? translators.hashCode() : 0;
    }
}
