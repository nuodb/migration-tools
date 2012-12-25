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

import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.Relational;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migration.jdbc.metadata.MetaDataType.ALL_TYPES;
import static com.nuodb.migration.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migration.jdbc.metadata.generator.ScriptType.DROP;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class ScriptGeneratorContext {

    private Dialect dialect;
    private String catalog;
    private String schema;
    private Collection<ScriptType> scriptTypes = newHashSet();
    private Collection<MetaDataType> metaDataTypes = newHashSet(ALL_TYPES);
    private Map<String, Object> attributes = newHashMap();
    private Map<Class<? extends Relational>, NamingStrategy<? extends Relational>> namingStrategyMap = newHashMap();
    private Map<Class<? extends Relational>, ScriptGenerator<? extends Relational>> scriptGeneratorMap = newHashMap();

    public ScriptGeneratorContext() {
        addScriptGenerator(new DatabaseGenerator());
        addScriptGenerator(new TableGenerator());
        addScriptGenerator(new SequenceGenerator());
        addScriptGenerator(new PrimaryKeyGenerator());
        addScriptGenerator(new IndexGenerator());
        addScriptGenerator(new ForeignKeyGenerator());

        // TODO: CDMT-41 fixed addNamingStrategy(new IndexNamingStrategy()); removed
        addNamingStrategy(new SequenceNamingStrategy());
        addNamingStrategy(new ForeignKeyNamingStrategy());
        addNamingStrategy(new HasIdentifierNamingStrategy());
    }

    public ScriptGeneratorContext(ScriptGeneratorContext scriptGeneratorContext) {
        this.dialect = scriptGeneratorContext.getDialect();
        this.catalog = scriptGeneratorContext.getCatalog();
        this.schema = scriptGeneratorContext.getSchema();

        this.scriptTypes.addAll(scriptGeneratorContext.getScriptTypes());
        this.metaDataTypes.addAll(scriptGeneratorContext.getMetaDataTypes());

        this.attributes.putAll(scriptGeneratorContext.getAttributes());
        this.namingStrategyMap.putAll(scriptGeneratorContext.getNamingStrategies());
        this.scriptGeneratorMap.putAll(scriptGeneratorContext.getScriptGenerators());
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public Collection<ScriptType> getScriptTypes() {
        return scriptTypes;
    }

    public void setScriptTypes(Collection<ScriptType> scriptTypes) {
        this.scriptTypes = scriptTypes;
    }

    public Collection<MetaDataType> getMetaDataTypes() {
        return this.metaDataTypes;
    }

    public void setMetaDataTypes(Collection<MetaDataType> metaDataTypes) {
        this.metaDataTypes = metaDataTypes;
    }

    public <R extends Relational> ScriptGenerator<R> getScriptGenerator(R relational) {
        return getGeneratorService(scriptGeneratorMap, relational);
    }

    public <R extends Relational> ScriptGenerator<R> getScriptGenerator(Class<R> relationalType) {
        return (ScriptGenerator<R>) scriptGeneratorMap.get(relationalType);
    }

    public <R extends Relational> void addScriptGenerator(ScriptGenerator<R> scriptGenerator) {
        scriptGeneratorMap.put(scriptGenerator.getRelationalType(), scriptGenerator);
    }

    public Map<Class<? extends Relational>, ScriptGenerator<? extends Relational>> getScriptGenerators() {
        return scriptGeneratorMap;
    }

    public <R extends Relational> void addNamingStrategy(NamingStrategy<R> namingStrategy) {
        namingStrategyMap.put(namingStrategy.getRelationalType(), namingStrategy);
    }

    public <R extends Relational> NamingStrategy<R> getNamingStrategy(R relational) {
        return getGeneratorService(namingStrategyMap, relational);
    }

    public Map<Class<? extends Relational>, NamingStrategy<? extends Relational>> getNamingStrategies() {
        return namingStrategyMap;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
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

    public <R extends Relational> Collection<String> getScripts(R relational) {
        return getScriptGenerator(relational).getScripts(relational, this);
    }

    public <R extends Relational> Collection<String> getCreateScripts(R relational) {
        ScriptGeneratorContext context = new ScriptGeneratorContext(this);
        context.setScriptTypes(newHashSet(CREATE));
        return getScriptGenerator(relational).getScripts(relational, this);
    }

    public <R extends Relational> Collection<String> getDropScripts(R relational) {
        ScriptGeneratorContext context = new ScriptGeneratorContext(this);
        context.setScriptTypes(newHashSet(DROP));
        return getScriptGenerator(relational).getScripts(relational, this);
    }

    public <R extends Relational> Collection<String> getDropCreateScripts(R relational) {
        ScriptGeneratorContext context = new ScriptGeneratorContext(this);
        context.setScriptTypes(newHashSet(DROP, CREATE));
        return getScriptGenerator(relational).getScripts(relational, this);
    }

    public <R extends Relational> String getName(R relational) {
        return getNamingStrategy(relational).getName(relational, this);
    }

    public <R extends Relational> String getName(R relational, boolean identifier) {
        return getNamingStrategy(relational).getName(relational, this, identifier);
    }

    public <R extends Relational> String getQualifiedName(R relational) {
        return getNamingStrategy(relational).getQualifiedName(relational, this);
    }

    public <R extends Relational> String getQualifiedName(R relational, boolean identifier) {
        return getNamingStrategy(relational).getQualifiedName(relational, this, identifier);
    }


}
