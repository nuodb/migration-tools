/**
 * Copyright (c) 2012, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in sourceDialect and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of sourceDialect code must retain the above copyright
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

import com.google.common.collect.Maps;

import java.util.Map;

import static org.apache.commons.lang3.StringUtils.trim;

/**
 * @author Sergey Bushik
 */
public class SimpleScriptTranslator implements ScriptTranslator {

    private final Map<String, String> scriptTranslations = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private final Dialect sourceDialect;
    private final Dialect targetDialect;

    public SimpleScriptTranslator(Dialect sourceDialect, Dialect targetDialect) {
        this.sourceDialect = sourceDialect;
        this.targetDialect = targetDialect;
    }

    @Override
    public String translateScript(String sourceScript) {
        return scriptTranslations.get(trim(sourceScript));
    }

    @Override
    public void addScriptTranslation(String sourceScript, String targetScript) {
        scriptTranslations.put(trim(sourceScript), trim(targetScript));
    }

    @Override
    public Dialect getSourceDialect() {
        return sourceDialect;
    }

    @Override
    public Dialect getTargetDialect() {
        return targetDialect;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleScriptTranslator that = (SimpleScriptTranslator) o;

        if (sourceDialect != null ? !sourceDialect.equals(that.sourceDialect) : that.sourceDialect != null) {
            return false;
        }
        if (targetDialect != null ? !targetDialect.equals(that.targetDialect) : that.targetDialect != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = sourceDialect != null ? sourceDialect.hashCode() : 0;
        result = 31 * result + (targetDialect != null ? targetDialect.hashCode() : 0);
        return result;
    }
}
