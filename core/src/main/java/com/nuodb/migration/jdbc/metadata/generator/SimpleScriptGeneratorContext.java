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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.Relational;

import java.util.Collection;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleScriptGeneratorContext implements ScriptGeneratorContext {

    private Dialect dialect;
    private String catalog;
    private String schema;
    private Map<Class<? extends Relational>, ScriptGenerator<? extends Relational>> scriptGenerators = Maps.newHashMap();
    private Collection<MetaDataType> metaDataTypes = Lists.newArrayList(MetaDataType.ALL_TYPES);

    public SimpleScriptGeneratorContext() {
        addScriptGenerator(new DatabaseGenerator());
        addScriptGenerator(new TableGenerator());
        addScriptGenerator(new PrimaryKeyGenerator());
        addScriptGenerator(new IndexGenerator());
        addScriptGenerator(new ForeignKeyGenerator());
    }

    public SimpleScriptGeneratorContext(ScriptGeneratorContext context) {
        this.dialect = context.getDialect();
        this.catalog = context.getCatalog();
        this.schema = context.getSchema();

        this.metaDataTypes = context.getMetaDataTypes();
        this.scriptGenerators.putAll(context.getScriptGenerators());
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
        scriptGenerators.put(scriptGenerator.getRelationalType(), scriptGenerator);
    }

    @Override
    public <R extends Relational> ScriptGenerator<R> getScriptGenerator(R relational) {
        Class<? extends Relational> objectType = relational.getClass();
        ScriptGenerator<R> schemaGenerator = null;
        while (schemaGenerator == null && objectType != null && Relational.class.isAssignableFrom(objectType)) {
            schemaGenerator = (ScriptGenerator<R>) getScriptGenerator(objectType);
            objectType = (Class<? extends Relational>) objectType.getSuperclass();
        }
        if (schemaGenerator == null) {
            throw new ScriptGeneratorException(format("Script generator not found for %s", relational.getClass()));
        }
        return schemaGenerator;
    }

    @Override
    public <R extends Relational> ScriptGenerator<R> getScriptGenerator(Class<R> objectType) {
        return (ScriptGenerator<R>) scriptGenerators.get(objectType);
    }

    @Override
    public Map<Class<? extends Relational>, ScriptGenerator<? extends Relational>> getScriptGenerators() {
        return scriptGenerators;
    }

    @Override
    public <R extends Relational> String[] getCreateSql(R relational) {
        return getScriptGenerator(relational).getCreateSql(relational, this);
    }

    @Override
    public <R extends Relational> String[] getDropSql(R relational) {
        return getScriptGenerator(relational).getDropSql(relational, this);
    }
}
