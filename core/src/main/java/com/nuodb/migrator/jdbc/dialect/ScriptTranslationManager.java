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

import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;

import java.util.Collection;
import java.util.regex.Pattern;

import static com.google.common.collect.Sets.newHashSet;

/**
 * @author Sergey Bushik
 */
public class ScriptTranslationManager {

    private Collection<ScriptTranslator> scriptTranslators = newHashSet();

    public void addScriptTranslation(DatabaseInfo sourceDatabaseInfo, String sourceScript,
                                     DatabaseInfo targetDatabaseInfo, String targetScript) {
        PatternScriptTranslator scriptTranslator = new PatternScriptTranslator(sourceDatabaseInfo, targetDatabaseInfo);
        scriptTranslator.addScriptTranslation(sourceScript, targetScript);
        addScriptTranslator(scriptTranslator);
    }

    public void addScriptTranslations(DatabaseInfo sourceDatabaseInfo, Collection<String> sourceScripts,
                                      DatabaseInfo targetDatabaseInfo, String targetScript) {
        PatternScriptTranslator scriptTranslator = new PatternScriptTranslator(sourceDatabaseInfo, targetDatabaseInfo);
        scriptTranslator.addScriptTranslations(sourceScripts, targetScript);
        addScriptTranslator(scriptTranslator);
    }

    public void addScriptTranslationRegex(DatabaseInfo sourceDatabaseInfo, String sourceScript,
                                          DatabaseInfo targetDatabaseInfo, String targetScript) {
        PatternScriptTranslator scriptTranslator = new PatternScriptTranslator(sourceDatabaseInfo, targetDatabaseInfo);
        scriptTranslator.addScriptTranslationRegex(sourceScript, targetScript);
        addScriptTranslator(scriptTranslator);
    }

    public void addScriptTranslationPattern(DatabaseInfo sourceDatabaseInfo, Pattern sourceScript,
                                            DatabaseInfo targetDatabaseInfo, String targetScript) {
        PatternScriptTranslator scriptTranslator = new PatternScriptTranslator(sourceDatabaseInfo, targetDatabaseInfo);
        scriptTranslator.addScriptTranslationPattern(sourceScript, targetScript);
        addScriptTranslator(scriptTranslator);
    }

    public void addScriptTranslator(ScriptTranslator scriptTranslator) {
        scriptTranslators.add(scriptTranslator);
    }

    public Collection<ScriptTranslator> getScriptTranslators() {
        return scriptTranslators;
    }

    public void setScriptTranslators(Collection<ScriptTranslator> scriptTranslators) {
        this.scriptTranslators = scriptTranslators;
    }

    public Script translateScript(Script sourceScript, DatabaseInfo targetDatabaseInfo) {
        Script targetScript = null;
        for (ScriptTranslator scriptTranslator : getScriptTranslators()) {
            if (scriptTranslator.canTranslateScript(sourceScript, targetDatabaseInfo)) {
                targetScript = scriptTranslator.translateScript(sourceScript, targetDatabaseInfo);
            }
            if (targetScript != null) {
                break;
            }
        }
        return targetScript;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ScriptTranslationManager)) return false;

        ScriptTranslationManager that = (ScriptTranslationManager) o;

        if (scriptTranslators != null ? !scriptTranslators.equals(
                that.scriptTranslators) : that.scriptTranslators != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return scriptTranslators != null ? scriptTranslators.hashCode() : 0;
    }
}
