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
package com.nuodb.migration.jdbc.metadata.generator;

import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.Relational;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleScriptGeneratorContext implements ScriptGeneratorContext {

    private Dialect dialect;
    private String catalog;
    private String schema;
    private Map<Class<? extends Relational>, NamingStrategy<? extends Relational>> namingStrategyMap = newHashMap();
    private Map<Class<? extends Relational>, ScriptGenerator<? extends Relational>> scriptGeneratorMap = newHashMap();
    private Collection<MetaDataType> metaDataTypes = newArrayList(MetaDataType.ALL_TYPES);
    private Map map = Maps.newHashMap();

    public SimpleScriptGeneratorContext() {
        addScriptGenerator(new DatabaseGenerator());
        addScriptGenerator(new TableGenerator());
        addScriptGenerator(new SequenceGenerator());
        addScriptGenerator(new PrimaryKeyGenerator());
        addScriptGenerator(new IndexGenerator());
        addScriptGenerator(new ForeignKeyGenerator());

        addNamingStrategy(new IndexNamingStrategy());
        addNamingStrategy(new SequenceNamingStrategy());
        addNamingStrategy(new ForeignKeyNamingStrategy());
        addNamingStrategy(new HasIdentifierNamingStrategy());
    }

    public SimpleScriptGeneratorContext(ScriptGeneratorContext scriptGeneratorContext) {
        this.dialect = scriptGeneratorContext.getDialect();
        this.catalog = scriptGeneratorContext.getCatalog();
        this.schema = scriptGeneratorContext.getSchema();

        this.metaDataTypes.addAll(scriptGeneratorContext.getMetaDataTypes());
        this.namingStrategyMap.putAll(scriptGeneratorContext.getNamingStrategies());
        this.scriptGeneratorMap.putAll(scriptGeneratorContext.getScriptGenerators());

        this.putAll(scriptGeneratorContext);
    }

    @Override
    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public Collection<MetaDataType> getMetaDataTypes() {
        return this.metaDataTypes;
    }

    @Override
    public void setMetaDataTypes(Collection<MetaDataType> metaDataTypes) {
        this.metaDataTypes = metaDataTypes;
    }

    @Override
    public <R extends Relational> void addScriptGenerator(ScriptGenerator<R> scriptGenerator) {
        scriptGeneratorMap.put(scriptGenerator.getRelationalType(), scriptGenerator);
    }

    @Override
    public <R extends Relational> ScriptGenerator<R> getScriptGenerator(R relational) {
        return getGeneratorService(scriptGeneratorMap, relational);
    }

    @Override
    public <R extends Relational> Collection<String> getCreateScripts(R relational) {
        return getScriptGenerator(relational).getCreateScripts(relational, this);
    }

    @Override
    public <R extends Relational> Collection<String> getDropScripts(R relational) {
        return getScriptGenerator(relational).getDropScripts(relational, this);
    }

    @Override
    public <R extends Relational> Collection<String> getDropCreateScripts(R relational) {
        return getScriptGenerator(relational).getDropCreateScripts(relational, this);
    }

    @Override
    public <R extends Relational> ScriptGenerator<R> getScriptGenerator(Class<R> relationalType) {
        return (ScriptGenerator<R>) scriptGeneratorMap.get(relationalType);
    }

    @Override
    public Map<Class<? extends Relational>, ScriptGenerator<? extends Relational>> getScriptGenerators() {
        return scriptGeneratorMap;
    }

    @Override
    public <R extends Relational> String getName(R relational) {
        return getNamingStrategy(relational).getName(relational, this);
    }

    @Override
    public <R extends Relational> String getName(R relational, boolean identifier) {
        return getNamingStrategy(relational).getName(relational, this, identifier);
    }

    @Override
    public <R extends Relational> String getQualifiedName(R relational) {
        return getNamingStrategy(relational).getQualifiedName(relational, this);
    }

    @Override
    public <R extends Relational> String getQualifiedName(R relational, boolean identifier) {
        return getNamingStrategy(relational).getQualifiedName(relational, this, identifier);
    }

    @Override
    public <R extends Relational> void addNamingStrategy(NamingStrategy<R> namingStrategy) {
        namingStrategyMap.put(namingStrategy.getRelationalType(), namingStrategy);
    }

    @Override
    public <R extends Relational> NamingStrategy<R> getNamingStrategy(R relational) {
        return getGeneratorService(namingStrategyMap, relational);
    }

    @Override
    public Map<Class<? extends Relational>, NamingStrategy<? extends Relational>> getNamingStrategies() {
        return namingStrategyMap;
    }

    @Override
    public Set entrySet() {
        return map.entrySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return map.get(key);
    }

    @Override
    public Object put(Object key, Object value) {
        return map.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(Map m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set keySet() {
        return map.keySet();
    }

    @Override
    public Collection values() {
        return map.values();
    }

    protected <R extends Relational, T extends GeneratorService<R>> T getGeneratorService(
            Map generatorServiceMap, R relational) {
        Class<? extends Relational> relationalType = relational.getClass();
        Class<? extends Relational> type = relationalType;
        GeneratorService<R> generatorService = null;
        while (generatorService == null && type != null) {
            generatorService = (GeneratorService<R>) generatorServiceMap.get(type);
            if (generatorService == null) {
                generatorService = getGeneratorService(generatorServiceMap, type.getInterfaces());
            }
            type = (Class<? extends Relational>) type.getSuperclass();
        }
        if (generatorService == null) {
            throw new ScriptGeneratorException(format("Generator service not found for %s", relationalType));
        }
        if (!relationalType.equals(generatorService.getRelationalType())) {
            generatorServiceMap.put(relationalType, generatorService);
        }
        return (T) generatorService;
    }

    protected <R extends Relational> GeneratorService<R> getGeneratorService(Map generatorServiceMap,
                                                                             Class<?>... types) {
        GeneratorService<R> generatorService = null;
        for (int i = 0, length = types.length; generatorService == null && i < length; i++) {
            generatorService = (GeneratorService<R>) generatorServiceMap.get(types[i]);
        }
        return generatorService;
    }
}
