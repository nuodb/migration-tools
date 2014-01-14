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
package com.nuodb.migrator.load;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptImporter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.IdentifiableBase.getQualifiedName;
import static com.nuodb.migrator.utils.Collections.addIgnoreNull;
import static java.lang.Math.min;
import static java.util.Collections.singleton;

/**
 * Generates USE "SCHEMA" statement
 *
 * @author Sergey Bushik
 */
public class SchemaScriptImporter implements ScriptImporter {

    private Dialect dialect;
    private String catalogName;
    private String schemaName;

    public SchemaScriptImporter(Dialect dialect, String catalogName, String schemaName) {
        this.dialect = dialect;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
    }

    @Override
    public void open() throws Exception {
    }

    @Override
    public Collection<String> importScripts() throws Exception {
        Dialect dialect = getDialect();
        String schema = getQualifiedName(dialect, getQualifiers(), null, null);
        String useSchema = null;
        if (dialect.supportsCatalogs()) {
            useSchema = dialect.getUseCatalog(schema);
        } else if (dialect.supportsSchemas()) {
            useSchema = dialect.getUseSchema(schema);
        }
        return useSchema != null ? singleton(useSchema) : Collections.<String>emptySet();
    }

    protected Collection<String> getQualifiers() {
        int maximum = 0;
        Dialect dialect = getDialect();
        if (dialect.supportsCatalogs()) {
            maximum++;
        }
        if (dialect.supportsSchemas()) {
            maximum++;
        }
        List<String> qualifiers = newArrayList();
        if (getCatalogName() != null || getSchemaName() != null) {
            addIgnoreNull(qualifiers, getCatalogName());
            addIgnoreNull(qualifiers, getSchemaName());
        }
        int actual = min(qualifiers.size(), maximum);
        return qualifiers.subList(qualifiers.size() - actual, qualifiers.size());
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public void close() throws Exception {
    }
}
