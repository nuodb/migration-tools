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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.jdbc.metadata.MetaDataHandlerUtils.getHandler;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TYPES;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.DROP;
import static com.nuodb.migrator.utils.Collections.newPrioritySet;
import static com.nuodb.migrator.utils.Priority.LOW;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class ScriptGeneratorManager {
    /**
     * Script generator manager attribute with supported table types for table
     * script generator, by default TABLE type is migrated only.
     */
    public static final String TABLE_TYPES = "table.types";
    /**
     * Contains Multimap<Table, ForeignKey> to share foreign keys pending
     * generation of primary table.
     */
    public static final String FOREIGN_KEYS = "foreign.keys";
    /**
     * If with "tables" attribute a non null collection is assigned, the
     * collection will hold a list of generated tables.
     */
    public static final String TABLES = "tables";
    /**
     * Script generator manager attribute which accepts true or false value. If
     * set to true results in in-lined statements for primary key, indexes and
     * foreign keys, default is false.
     */
    public static final String SCRIPTS_IN_CREATE_TABLE = "scripts.in.create.table";
    /**
     * Script generator manager attribute which accepts true or false value. If
     * set to true results in in-lined statements for unique constraints,
     * default is true. see {@code HasTablesScriptGenerator}
     */
    public static final String UNIQUE_CONSTRAINTS = "unique.constraints";

    private String sourceCatalog;
    private String sourceSchema;
    private Session sourceSession;
    private String targetCatalog;
    private String targetSchema;
    private Dialect targetDialect;

    private Map<String, Object> attributes = newHashMap();
    private PrioritySet<NamingStrategy<? extends MetaData>> namingStrategies = newPrioritySet();
    private PrioritySet<ScriptGenerator<? extends MetaData>> scriptGenerators = newPrioritySet();
    private Collection<ScriptType> scriptTypes = newHashSet(ScriptType.values());
    private Collection<MetaDataType> objectTypes = newHashSet(TYPES);
    private MetaDataFilterManager metaDataFilterManager = new MetaDataFilterManager();

    public ScriptGeneratorManager() {
        addScriptGenerator(new HasTablesScriptGenerator(), LOW);
        addScriptGenerator(new HasSchemasScriptGenerator());
        addScriptGenerator(new TableScriptGenerator());
        addScriptGenerator(new SequenceScriptGenerator());
        addScriptGenerator(new PrimaryKeyScriptGenerator());
        addScriptGenerator(new IndexScriptGenerator());
        addScriptGenerator(new ForeignKeyScriptGenerator());
        addScriptGenerator(new TriggerScriptGenerator());

        addNamingStrategy(new IndexAutoNamingStrategy());
        addNamingStrategy(new SequenceAutoNamingStrategy());
        addNamingStrategy(new ForeignKeyAutoNamingStrategy());
        addNamingStrategy(new TriggerAutoNamingStrategy());
        addNamingStrategy(new SourceNamingStrategy(), LOW);
    }

    public ScriptGeneratorManager(ScriptGeneratorManager scriptGeneratorManager) {
        sourceCatalog = scriptGeneratorManager.getSourceCatalog();
        sourceSchema = scriptGeneratorManager.getSourceSchema();
        sourceSession = scriptGeneratorManager.getSourceSession();
        targetCatalog = scriptGeneratorManager.getTargetCatalog();
        targetSchema = scriptGeneratorManager.getTargetSchema();
        targetDialect = scriptGeneratorManager.getTargetDialect();

        attributes = newHashMap(scriptGeneratorManager.getAttributes());
        scriptTypes = newHashSet(scriptGeneratorManager.getScriptTypes());
        objectTypes = newHashSet(scriptGeneratorManager.getObjectTypes());

        namingStrategies.addAll(scriptGeneratorManager.getNamingStrategies());
        scriptGenerators.addAll(scriptGeneratorManager.getScriptGenerators());

        metaDataFilterManager = scriptGeneratorManager.getMetaDataFilterManager();
    }

    public String getName(MetaData object) {
        return getNamingStrategy(object).getName(object, this, true);
    }

    public String getName(MetaData object, boolean normalize) {
        return getNamingStrategy(object).getName(object, this, normalize);
    }

    public String getQualifiedName(MetaData object) {
        return getNamingStrategy(object).getQualifiedName(object, this, getTargetCatalog(), getTargetSchema(), true);
    }

    public String getQualifiedName(MetaData object, boolean normalize) {
        return getNamingStrategy(object).getQualifiedName(object, this, getTargetCatalog(), getTargetSchema(),
                normalize);
    }

    public String getQualifiedName(MetaData object, String catalog, String schema, boolean normalize) {
        return getNamingStrategy(object).getQualifiedName(object, this, catalog, schema, normalize);
    }

    public void addNamingStrategy(NamingStrategy<? extends MetaData> namingStrategy) {
        namingStrategies.add(namingStrategy);
    }

    public void addNamingStrategy(NamingStrategy<? extends MetaData> namingStrategy, int priority) {
        namingStrategies.add(namingStrategy, priority);
    }

    public NamingStrategy getNamingStrategy(MetaData object) {
        return (NamingStrategy) getHandler(namingStrategies, object);
    }

    public NamingStrategy getNamingStrategy(MetaDataType objectType) {
        return (NamingStrategy) getHandler(namingStrategies, objectType);
    }

    public void addScriptGenerator(ScriptGenerator<? extends MetaData> scriptGenerator) {
        scriptGenerators.add(scriptGenerator);
    }

    public void addScriptGenerator(ScriptGenerator<? extends MetaData> scriptGenerator, int priority) {
        scriptGenerators.add(scriptGenerator, priority);
    }

    public ScriptGenerator getScriptGenerator(MetaData object) {
        return (ScriptGenerator) getHandler(scriptGenerators, object);
    }

    public Collection<Script> getScripts(MetaData object) {
        return getScriptGenerator(object).getScripts(object, this);
    }

    public Collection<Script> getCreateScripts(MetaData object) {
        ScriptGeneratorManager context = new ScriptGeneratorManager(this);
        context.setScriptTypes(newHashSet(CREATE));
        return getScriptGenerator(object).getScripts(object, context);
    }

    public Collection<Script> getDropScripts(MetaData object) {
        ScriptGeneratorManager context = new ScriptGeneratorManager(this);
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

    public Dialect getTargetDialect() {
        return targetDialect;
    }

    public void setTargetDialect(Dialect targetDialect) {
        this.targetDialect = targetDialect;
    }

    public Session getSourceSession() {
        return sourceSession;
    }

    public void setSourceSession(Session sourceSession) {
        this.sourceSession = sourceSession;
    }

    public Collection<ScriptType> getScriptTypes() {
        return scriptTypes;
    }

    public void setScriptTypes(Collection<ScriptType> scriptTypes) {
        this.scriptTypes = scriptTypes;
    }

    public Object getAttribute(String attribute) {
        return attributes.get(attribute);
    }

    public void addAttribute(String attribute, Object value) {
        attributes.put(attribute, value);
    }

    public void removeAttribute(String attribute) {
        attributes.remove(attribute);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Collection<MetaDataType> getObjectTypes() {
        return this.objectTypes;
    }

    public void setObjectTypes(Collection<MetaDataType> objectTypes) {
        this.objectTypes = objectTypes;
    }

    public MetaDataFilterManager getMetaDataFilterManager() {
        return metaDataFilterManager;
    }

    public void setMetaDataFilterManager(MetaDataFilterManager metaDataFilterManager) {
        this.metaDataFilterManager = metaDataFilterManager;
    }

    public PrioritySet<NamingStrategy<? extends MetaData>> getNamingStrategies() {
        return namingStrategies;
    }

    public PrioritySet<ScriptGenerator<? extends MetaData>> getScriptGenerators() {
        return scriptGenerators;
    }
}
