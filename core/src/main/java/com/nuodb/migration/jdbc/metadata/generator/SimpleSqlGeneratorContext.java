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
public class SimpleSqlGeneratorContext implements SqlGeneratorContext {

    private Dialect dialect;
    private String catalog;
    private String schema;
    private Map<Class<? extends Relational>, SqlGenerator<? extends Relational>> sqlGeneratorMap = Maps.newHashMap();
    private Map<Class<? extends Relational>, NamingStrategy<? extends Relational>> namingStrategyMap = Maps.newHashMap();
    private Collection<MetaDataType> metaDataTypes = Lists.newArrayList(MetaDataType.ALL_TYPES);

    public SimpleSqlGeneratorContext() {
        addSqlGenerator(new DatabaseGenerator());
        addSqlGenerator(new TableGenerator());
        addSqlGenerator(new PrimaryKeyGenerator());
        addSqlGenerator(new IndexGenerator());
        addSqlGenerator(new ForeignKeyGenerator());

        addNamingStrategy(new TableNamingStrategy());
        addNamingStrategy(new IndexNamingStrategy());
        addNamingStrategy(new ForeignKeyNamingStrategy());
        addNamingStrategy(new HasIdentifierNamingStrategy());
    }

    public SimpleSqlGeneratorContext(SqlGeneratorContext context) {
        this.dialect = context.getDialect();
        this.catalog = context.getCatalog();
        this.schema = context.getSchema();

        this.metaDataTypes = context.getMetaDataTypes();
        this.sqlGeneratorMap.putAll(context.getSqlGenerators());
        this.namingStrategyMap.putAll(context.getNamingStrategies());
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
    public <R extends Relational> void addSqlGenerator(SqlGenerator<R> sqlGenerator) {
        sqlGeneratorMap.put(sqlGenerator.getObjectType(), sqlGenerator);
    }

    @Override
    public <R extends Relational> SqlGenerator<R> getSqlGenerator(R object) {
        return getGeneratorService(sqlGeneratorMap, object);
    }

    @Override
    public <R extends Relational> String[] getCreateSql(R object) {
        return getSqlGenerator(object).getCreateSql(object, this);
    }

    @Override
    public <R extends Relational> String[] getDropSql(R object) {
        return getSqlGenerator(object).getDropSql(object, this);
    }

    @Override
    public <R extends Relational> SqlGenerator<R> getSqlGenerator(Class<R> objectType) {
        return (SqlGenerator<R>) sqlGeneratorMap.get(objectType);
    }

    @Override
    public Map<Class<? extends Relational>, SqlGenerator<? extends Relational>> getSqlGenerators() {
        return sqlGeneratorMap;
    }

    @Override
    public <R extends Relational> String getName(R object) {
        return getNamingStrategy(object).getName(object, this);
    }

    @Override
    public <R extends Relational> String getIdentifier(R object) {
        String name = getName(object);
        return getDialect().getIdentifier(name);
    }

    @Override
    public <R extends Relational> void addNamingStrategy(NamingStrategy<R> namingStrategy) {
        namingStrategyMap.put(namingStrategy.getObjectType(), namingStrategy);
    }

    @Override
    public <R extends Relational> NamingStrategy<R> getNamingStrategy(R object) {
        return getGeneratorService(namingStrategyMap, object);
    }

    @Override
    public Map<Class<? extends Relational>, NamingStrategy<? extends Relational>> getNamingStrategies() {
        return namingStrategyMap;
    }

    protected <R extends Relational, T extends GeneratorService<R>> T getGeneratorService(
            Map generatorServiceMap, R object) {
        Class<? extends Relational> objectType = object.getClass();
        Class<? extends Relational> type = objectType;
        GeneratorService<R> generatorService = null;
        while (generatorService == null && type != null) {
            generatorService = (GeneratorService<R>) generatorServiceMap.get(type);
            if (generatorService == null) {
                generatorService = getGeneratorService(generatorServiceMap, type.getInterfaces());
            }
            type = (Class<? extends Relational>) type.getSuperclass();
        }
        if (generatorService == null) {
            throw new ScriptGeneratorException(format("Generator service not found for %s", objectType));
        }
        if (!objectType.equals(generatorService.getObjectType())) {
            generatorServiceMap.put(objectType, generatorService);
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
