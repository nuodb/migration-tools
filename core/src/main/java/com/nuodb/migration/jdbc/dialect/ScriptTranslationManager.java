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
import com.nuodb.migration.jdbc.resolve.ServiceResolver;
import com.nuodb.migration.jdbc.resolve.SimpleServiceResolverAware;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.Multimaps.newSetMultimap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;

/**
 * @author Sergey Bushik
 */
public class ScriptTranslationManager extends SimpleServiceResolverAware<Dialect> {

    private Set<ScriptTranslation> scriptTranslations = newLinkedHashSet();

    private Multimap<Dialect, ScriptTranslator> scriptTranslatorsMap = newSetMultimap(
            Maps.<Dialect, Collection<ScriptTranslator>>newHashMap(),
            new Supplier<Set<ScriptTranslator>>() {
                @Override
                public Set<ScriptTranslator> get() {
                    return newHashSet();
                }
            });

    public Script getScriptTranslation(DatabaseInfo sourceDatabaseInfo, DatabaseInfo targetDatabaseInfo,
                                       Script sourceScript) {
        DialectResolver dialectResolver = getDialectResolver();
        Script scriptTranslation = null;
        if (dialectResolver != null) {
            Dialect sourceDialect = dialectResolver.resolve(sourceDatabaseInfo);
            Dialect targetDialect = dialectResolver.resolve(targetDatabaseInfo);
            scriptTranslation = getScriptTranslation(sourceDialect, targetDialect, sourceScript);
        } else {
            for (ScriptTranslation scriptTranslationEntry : scriptTranslations) {
                if (scriptTranslationEntry.getSourceDatabaseInfo().equals(sourceDatabaseInfo) &&
                        scriptTranslationEntry.getTargetDatabaseInfo().equals(targetDatabaseInfo) &&
                        sourceScript.equals(sourceScript)) {
                    scriptTranslation = scriptTranslationEntry.getTargetScript();
                    break;
                }
            }
        }
        return scriptTranslation;
    }

    public Script getScriptTranslation(Dialect sourceDialect, Dialect targetDialect, Script sourceScript) {
        ScriptTranslator targetScriptTranslator = null;
        Collection<ScriptTranslator> scriptTranslators = scriptTranslatorsMap.get(sourceDialect);
        Script targetScript = null;
        for (ScriptTranslator scriptTranslator : scriptTranslators) {
            if (scriptTranslator.getTargetDialect().equals(targetDialect)) {
                targetScript = targetScriptTranslator.translateScript(sourceScript);
                break;
            }
        }
        if (targetScript == null) {
            for (ScriptTranslation scriptTranslation : scriptTranslations) {
                boolean sourceDialectInfoMatches = scriptTranslation.getSourceDialect() != null ?
                        scriptTranslation.getSourceDialect().equals(sourceDialect) :
                        scriptTranslation.getSourceDatabaseInfo().matches(sourceDialect.getDatabaseInfo());
                boolean targetDialectInfoMatches = scriptTranslation.getTargetDialect() != null ?
                        scriptTranslation.getTargetDialect().equals(targetDialect) :
                        scriptTranslation.getTargetDatabaseInfo().matches(sourceDialect.getDatabaseInfo());
                boolean sourceScriptEquals = ObjectUtils.equals(scriptTranslation.getSourceScript(), sourceScript);
                if (sourceDialectInfoMatches && targetDialectInfoMatches && sourceScriptEquals) {
                    targetScript = scriptTranslation.getTargetScript();
                    break;
                }
            }
        }
        return targetScript != null ? targetScript : sourceScript;
    }

    public void addScriptTranslation(ScriptTranslation scriptTranslation) {
        DialectResolver dialectResolver = getDialectResolver();
        if (dialectResolver != null) {
            Dialect sourceDialect = scriptTranslation.getSourceDialect() != null ?
                    scriptTranslation.getSourceDialect() :
                    dialectResolver.resolve(scriptTranslation.getSourceDatabaseInfo());
            Dialect targetDialect = scriptTranslation.getTargetDialect() != null ?
                    scriptTranslation.getTargetDialect() :
                    dialectResolver.resolve(scriptTranslation.getTargetDatabaseInfo());
            Collection<ScriptTranslator> sourceScriptTranslators = scriptTranslatorsMap.get(sourceDialect);
            ScriptTranslator scriptTranslator = null;
            for (ScriptTranslator sourceScriptTranslator : sourceScriptTranslators) {
                if (sourceScriptTranslator.getTargetDialect().equals(targetDialect)) {
                    scriptTranslator = sourceScriptTranslator;
                    break;
                }
            }
            if (scriptTranslator == null) {
                sourceScriptTranslators.add(
                        scriptTranslator = new SimpleScriptTranslator(sourceDialect, targetDialect));
            }
            scriptTranslator.addScriptTranslation(
                    scriptTranslation.getSourceScript(), scriptTranslation.getTargetScript());
        } else {
            scriptTranslations.add(scriptTranslation);
        }
    }

    @Override
    public ServiceResolver<Dialect> getServiceResolver() {
        return super.getServiceResolver();
    }

    @Override
    public void setServiceResolver(ServiceResolver<Dialect> serviceResolver) {
        super.setServiceResolver(serviceResolver);

        DialectResolver dialectResolver = getDialectResolver();
        if (dialectResolver != null) {
            for (ScriptTranslation scriptTranslation : scriptTranslations) {
                addScriptTranslation(scriptTranslation);
            }
            scriptTranslations.clear();
        }
    }

    protected DialectResolver getDialectResolver() {
        return (DialectResolver) getServiceResolver();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptTranslationManager that = (ScriptTranslationManager) o;
        DialectResolver dialectResolver = getDialectResolver();
        if (dialectResolver != null ? !dialectResolver.equals(
                that.getDialectResolver()) : that.getDialectResolver() != null)
            return false;
        if (scriptTranslations != null ? !scriptTranslations.equals(
                that.scriptTranslations) : that.scriptTranslations != null) return false;
        if (scriptTranslatorsMap != null ? !scriptTranslatorsMap.equals(
                that.scriptTranslatorsMap) : that.scriptTranslatorsMap != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getDialectResolver() != null ? getDialectResolver().hashCode() : 0;
        result = 31 * result + (scriptTranslations != null ? scriptTranslations.hashCode() : 0);
        result = 31 * result + (scriptTranslatorsMap != null ? scriptTranslatorsMap.hashCode() : 0);
        return result;
    }
}
