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
package com.nuodb.migration.jdbc.dialect;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.nuodb.migration.jdbc.resolve.DatabaseInfo;

import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.Multimaps.newSetMultimap;
import static com.google.common.collect.Sets.newHashSet;

/**
 * @author Sergey Bushik
 */
public class ScriptTranslatorManager {

    private DialectResolver dialectResolver;

    private Multimap<Dialect, ScriptTranslator> sourceScriptTranslatorsMap = newSetMultimap(
            Maps.<Dialect, Collection<ScriptTranslator>>newHashMap(),
            new Supplier<Set<ScriptTranslator>>() {
                @Override
                public Set<ScriptTranslator> get() {
                    return newHashSet();
                }
            });

    public void addScriptTranslation(DatabaseInfo sourceDatabaseInfo, DatabaseInfo targetDatabaseInfo,
                                     String sourceScript, String targetScript) {
        Dialect sourceDialect = getDialectResolver().resolve(sourceDatabaseInfo);
        Dialect targetDialect = getDialectResolver().resolve(targetDatabaseInfo);
        addScriptTranslation(sourceDialect, targetDialect, sourceScript, targetScript);
    }

    public void addScriptTranslation(Dialect sourceDialect, Dialect targetDialect,
                                     String sourceScript, String targetScript) {
        Collection<ScriptTranslator> sourceScriptTranslators = sourceScriptTranslatorsMap.get(targetDialect);
        ScriptTranslator scriptTranslator = null;
        for (ScriptTranslator sourceScriptTranslator : sourceScriptTranslators) {
            if (sourceScriptTranslator.getSourceDialect().equals(targetDialect)) {
                scriptTranslator = sourceScriptTranslator;
                break;
            }

        }
        if (scriptTranslator == null) {
            sourceScriptTranslators.add(scriptTranslator = new SimpleScriptTranslator(sourceDialect, targetDialect));
        }
        scriptTranslator.addScriptTranslation(sourceScript, targetScript);
    }

    public void addScriptTranslator(ScriptTranslator scriptTranslator) {
        sourceScriptTranslatorsMap.put(scriptTranslator.getSourceDialect(), scriptTranslator);
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptTranslatorManager that = (ScriptTranslatorManager) o;

        if (dialectResolver != null ? !dialectResolver.equals(that.dialectResolver) : that.dialectResolver != null) {
            return false;
        }
        if (sourceScriptTranslatorsMap != null ? !sourceScriptTranslatorsMap.equals(
                that.sourceScriptTranslatorsMap) : that.sourceScriptTranslatorsMap != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = dialectResolver != null ? dialectResolver.hashCode() : 0;
        result = 31 * result + (sourceScriptTranslatorsMap != null ? sourceScriptTranslatorsMap.hashCode() : 0);
        return result;
    }
}
