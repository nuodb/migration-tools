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
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;

/**
 * @author Sergey Bushik
 */
public class SimpleTranslationContext implements TranslationContext {

    private DatabaseInfo databaseInfo;
    private TranslationManager translationManager;
    private Map<Object, Object> context;

    public SimpleTranslationContext(DatabaseInfo databaseInfo, TranslationManager translationManager) {
        this(databaseInfo, translationManager, null);
    }

    public SimpleTranslationContext(DatabaseInfo databaseInfo, TranslationManager translationManager,
                                    Map<Object, Object> context) {
        this.databaseInfo = databaseInfo;
        this.translationManager = translationManager;
        this.context = context != null ? context : newHashMap();
    }

    @Override
    public int size() {
        return context.size();
    }

    @Override
    public boolean isEmpty() {
        return context.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return context.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return context.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return context.get(key);
    }

    @Override
    public Object put(Object key, Object value) {
        return context.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return context.remove(key);
    }

    @Override
    public void putAll(Map<?, ?> m) {
        context.putAll(m);
    }

    @Override
    public void clear() {
        context.clear();
    }

    @Override
    public Set<Object> keySet() {
        return context.keySet();
    }

    @Override
    public Collection<Object> values() {
        return context.values();
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
        return context.entrySet();
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }

    @Override
    public TranslationManager getTranslationManager() {
        return translationManager;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleTranslationContext that = (SimpleTranslationContext) o;

        if (context != null ? !context.equals(that.context) : that.context != null) return false;
        if (translationManager != null ? !translationManager.equals(that.translationManager) :
                that.translationManager != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = translationManager != null ? translationManager.hashCode() : 0;
        result = 31 * result + (context != null ? context.hashCode() : 0);
        return result;
    }
}
