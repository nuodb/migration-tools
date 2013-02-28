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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.utils.Priority;
import com.nuodb.migrator.utils.PriorityList;
import com.nuodb.migrator.utils.SimplePriorityList;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.jdbc.metadata.MetaDataHandlerUtils.findMetaDataHandler;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.DROP;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class ScriptGeneratorContext {

    private String sourceCatalog;
    private String sourceSchema;
    private String targetCatalog;
    private String targetSchema;

    private Dialect dialect;

    private Map<String, Object> attributes = newHashMap();
    private PriorityList<NamingStrategy<? extends MetaData>> namingStrategies =
            new SimplePriorityList<NamingStrategy<? extends MetaData>>();
    private PriorityList<ScriptGenerator<? extends MetaData>> scriptGenerators =
            new SimplePriorityList<ScriptGenerator<? extends MetaData>>();

    private Collection<ScriptType> scriptTypes = newHashSet(ScriptType.values());
    private Collection<MetaDataType> metaDataTypes = newHashSet(MetaDataType.TYPES);

    public ScriptGeneratorContext() {
        addScriptGenerator(new HasTablesScriptGenerator(), Priority.LOW);
        addScriptGenerator(new HasSchemasScriptGenerator());
        addScriptGenerator(new TableScriptGenerator());
        addScriptGenerator(new SequenceScriptGenerator());
        addScriptGenerator(new PrimaryScriptGenerator());
        addScriptGenerator(new IndexScriptGenerator());
        addScriptGenerator(new ForeignKeyScriptGenerator());

        addNamingStrategy(new IndexNamingStrategy());
        addNamingStrategy(new SequenceNamingStrategy());
        addNamingStrategy(new ForeignKeyNamingStrategy());
        addNamingStrategy(new IdentifiableNamingStrategy(), Priority.LOW);
    }

    public ScriptGeneratorContext(ScriptGeneratorContext scriptGeneratorContext) {
        sourceCatalog = scriptGeneratorContext.getSourceCatalog();
        sourceSchema = scriptGeneratorContext.getSourceSchema();
        targetCatalog = scriptGeneratorContext.getTargetCatalog();
        targetSchema = scriptGeneratorContext.getTargetSchema();

        dialect = scriptGeneratorContext.getDialect();

        attributes = newHashMap(scriptGeneratorContext.getAttributes());
        scriptTypes = newHashSet(scriptGeneratorContext.getScriptTypes());
        metaDataTypes = newHashSet(scriptGeneratorContext.getMetaDataTypes());

        namingStrategies.addAll(scriptGeneratorContext.getNamingStrategies());
        scriptGenerators.addAll(scriptGeneratorContext.getScriptGenerators());
    }

    public String getName(MetaData metaData) {
        return getNamingStrategy(metaData).getName(metaData, this, true);
    }

    public String getName(MetaData metaData, boolean normalize) {
        return getNamingStrategy(metaData).getName(metaData, this, normalize);
    }

    public String getQualifiedName(MetaData metaData) {
        return getNamingStrategy(metaData).getQualifiedName(metaData, this, true);
    }

    public String getQualifiedName(MetaData metaData, boolean normalize) {
        return getNamingStrategy(metaData).getQualifiedName(metaData, this, normalize);
    }

    public void addNamingStrategy(NamingStrategy<? extends MetaData> namingStrategy) {
        namingStrategies.add(namingStrategy);
    }

    public void addNamingStrategy(NamingStrategy<? extends MetaData> namingStrategy, int priority) {
        namingStrategies.add(namingStrategy, priority);
    }

    public NamingStrategy getNamingStrategy(MetaData metaData) {
        return (NamingStrategy) findMetaDataHandler(namingStrategies,
                metaData.getObjectType());
    }

    public void addScriptGenerator(ScriptGenerator<? extends MetaData> scriptGenerator) {
        scriptGenerators.add(scriptGenerator);
    }

    public void addScriptGenerator(ScriptGenerator<? extends MetaData> scriptGenerator, int priority) {
        scriptGenerators.add(scriptGenerator, priority);
    }

    public ScriptGenerator getScriptGenerator(MetaData object) {
        return (ScriptGenerator) findMetaDataHandler(scriptGenerators, object.getObjectType());
    }

    public Collection<String> getScripts(MetaData object) {
        return getScriptGenerator(object).getScripts(object, this);
    }

    public Collection<String> getCreateScripts(MetaData object) {
        ScriptGeneratorContext context = new ScriptGeneratorContext(this);
        context.setScriptTypes(newHashSet(CREATE));
        return getScriptGenerator(object).getScripts(object, context);
    }

    public Collection<String> getDropScripts(MetaData object) {
        ScriptGeneratorContext context = new ScriptGeneratorContext(this);
        context.setScriptTypes(newHashSet(DROP));
        return getScriptGenerator(object).getScripts(object, context);
    }

    public String getSourceCatalog() {
        return sourceCatalog;
    }

    public void setSourceCatalog(String sourceCatalog) {
        this.sourceCatalog = sourceCatalog;
    }

    public String getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public String getTargetCatalog() {
        return targetCatalog;
    }

    public void setTargetCatalog(String targetCatalog) {
        this.targetCatalog = targetCatalog;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema;
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Collection<MetaDataType> getMetaDataTypes() {
        return this.metaDataTypes;
    }

    public void setMetaDataTypes(Collection<MetaDataType> metaDataTypes) {
        this.metaDataTypes = metaDataTypes;
    }

    public PriorityList<NamingStrategy<? extends MetaData>> getNamingStrategies() {
        return namingStrategies;
    }

    public PriorityList<ScriptGenerator<? extends MetaData>> getScriptGenerators() {
        return scriptGenerators;
    }
}
