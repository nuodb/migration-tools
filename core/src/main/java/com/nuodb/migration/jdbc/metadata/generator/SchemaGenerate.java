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
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.Relational;
import com.nuodb.migration.jdbc.metadata.MetaDataType;

import java.util.Collection;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class SchemaGenerate {

    private SqlGeneratorContext sqlGeneratorContext;
    private SqlExporter sqlExporter = StdOutSqlExporter.INSTANCE;

    public SchemaGenerate() {
        this(new SimpleSqlGeneratorContext());
    }

    public SchemaGenerate(SqlGeneratorContext sqlGeneratorContext) {
        this.sqlGeneratorContext = sqlGeneratorContext;
    }

    public void generate(Database database) throws Exception {
        try {
            sqlExporter.open();

            String[] dropSql = sqlGeneratorContext.getDropSql(database);
            sqlExporter.export(dropSql);

            String[] createSql = sqlGeneratorContext.getCreateSql(database);
            sqlExporter.export(createSql);
        } finally {
            sqlExporter.close();
        }
    }

    public Dialect getDialect() {
        return sqlGeneratorContext.getDialect();
    }

    public void setDialect(Dialect dialect) {
        sqlGeneratorContext.setDialect(dialect);
    }

    public String getCatalog() {
        return sqlGeneratorContext.getCatalog();
    }

    public void setCatalog(String catalog) {
        sqlGeneratorContext.setCatalog(catalog);
    }

    public String getSchema() {
        return sqlGeneratorContext.getSchema();
    }

    public void setSchema(String catalog) {
        sqlGeneratorContext.setSchema(catalog);
    }

    public SqlExporter getSqlExporter() {
        return sqlExporter;
    }

    public void setSqlExporter(SqlExporter sqlExporter) {
        this.sqlExporter = sqlExporter;
    }

    public <R extends Relational> void addScriptGenerator(SqlGenerator<R> sqlGenerator) {
        sqlGeneratorContext.addSqlGenerator(sqlGenerator);
    }

    public <R extends Relational> SqlGenerator<R> getScriptGenerator(R relational) {
        return sqlGeneratorContext.getSqlGenerator(relational);
    }

    public <R extends Relational> SqlGenerator<R> getScriptGenerator(Class<R> objectType) {
        return sqlGeneratorContext.getSqlGenerator(objectType);
    }

    public Map<Class<? extends Relational>, SqlGenerator<? extends Relational>> getScriptGenerators() {
        return sqlGeneratorContext.getSqlGenerators();
    }

    public void setMetaDataTypes(Collection<MetaDataType> metaDataTypes) {
        sqlGeneratorContext.setMetaDataTypes(metaDataTypes);
    }

    public Collection<MetaDataType> getMetaDataTypes() {
        return sqlGeneratorContext.getMetaDataTypes();
    }
}
