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
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.metadata.Relational;

import java.util.Collection;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class ScriptGenerate {

    private ScriptGeneratorContext scriptGeneratorContext;
    private ScriptExporter scriptExporter = StdOutScriptExporter.INSTANCE;

    public ScriptGenerate() {
        this(new SimpleScriptGeneratorContext());
    }

    public ScriptGenerate(ScriptGeneratorContext scriptGeneratorContext) {
        this.scriptGeneratorContext = scriptGeneratorContext;
    }

    public void generate(Database database) throws Exception {
        try {
            scriptExporter.open();

            String[] dropSql = scriptGeneratorContext.getDropSql(database);
            scriptExporter.export(dropSql);

            String[] createSql = scriptGeneratorContext.getCreateSql(database);
            scriptExporter.export(createSql);
        } finally {
            scriptExporter.close();
        }
    }

    public Dialect getDialect() {
        return scriptGeneratorContext.getDialect();
    }

    public void setDialect(Dialect dialect) {
        scriptGeneratorContext.setDialect(dialect);
    }

    public String getCatalog() {
        return scriptGeneratorContext.getCatalog();
    }

    public void setCatalog(String catalog) {
        scriptGeneratorContext.setCatalog(catalog);
    }

    public String getSchema() {
        return scriptGeneratorContext.getSchema();
    }

    public void setSchema(String catalog) {
        scriptGeneratorContext.setSchema(catalog);
    }

    public ScriptExporter getScriptExporter() {
        return scriptExporter;
    }

    public void setScriptExporter(ScriptExporter scriptExporter) {
        this.scriptExporter = scriptExporter;
    }

    public <R extends Relational> void addScriptGenerator(ScriptGenerator<R> scriptGenerator) {
        scriptGeneratorContext.addScriptGenerator(scriptGenerator);
    }

    public <R extends Relational> ScriptGenerator<R> getScriptGenerator(R relational) {
        return scriptGeneratorContext.getScriptGenerator(relational);
    }

    public <R extends Relational> ScriptGenerator<R> getScriptGenerator(Class<R> objectType) {
        return scriptGeneratorContext.getScriptGenerator(objectType);
    }

    public Map<Class<? extends Relational>, ScriptGenerator<? extends Relational>> getScriptGenerators() {
        return scriptGeneratorContext.getScriptGenerators();
    }

    public void setMetaDataTypes(Collection<MetaDataType> metaDataTypes) {
        scriptGeneratorContext.setMetaDataTypes(metaDataTypes);
    }

    public Collection<MetaDataType> getMetaDataTypes() {
        return scriptGeneratorContext.getMetaDataTypes();
    }
}
