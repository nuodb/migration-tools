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
import com.nuodb.migrator.jdbc.metadata.HasSchemas;
import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.Schema;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;

/**
 * @author Sergey Bushik
 */
public class HasSchemasScriptGenerator extends HasTablesScriptGenerator<HasSchemas> {

    public HasSchemasScriptGenerator() {
        super(HasSchemas.class);
    }

    @Override
    public Collection<String> getCreateScripts(HasSchemas hasSchemas, ScriptGeneratorManager scriptGeneratorManager) {
        Map<Schema, Collection<String>> schemaScripts = newLinkedHashMap();
        for (Schema schema : getSchemas(hasSchemas, scriptGeneratorManager)) {
            Collection<String> scripts = getHasTablesCreateScripts(schema, scriptGeneratorManager);
            if (!scripts.isEmpty()) {
                schemaScripts.put(schema, scripts);
            }
        }
        return getScripts(schemaScripts, scriptGeneratorManager);
    }

    @Override
    public Collection<String> getDropScripts(HasSchemas hasSchemas, ScriptGeneratorManager scriptGeneratorManager) {
        Map<Schema, Collection<String>> schemaScripts = newLinkedHashMap();
        for (Schema schema : getSchemas(hasSchemas, scriptGeneratorManager)) {
            Collection<String> scripts = getHasTablesDropScripts(schema, scriptGeneratorManager);
            if (!scripts.isEmpty()) {
                schemaScripts.put(schema, scripts);
            }
        }
        return getScripts(schemaScripts, scriptGeneratorManager);
    }

    @Override
    public Collection<String> getDropCreateScripts(HasSchemas hasSchemas,
                                                   ScriptGeneratorManager scriptGeneratorManager) {
        Map<Schema, Collection<String>> schemaScripts = newLinkedHashMap();
        for (Schema schema : getSchemas(hasSchemas, scriptGeneratorManager)) {
            Collection<String> scripts = getHasTablesDropCreateScripts(schema, scriptGeneratorManager);
            if (!scripts.isEmpty()) {
                schemaScripts.put(schema, scripts);
            }
        }
        return getScripts(schemaScripts, scriptGeneratorManager);
    }

    protected Collection<String> getScripts(Map<Schema, Collection<String>> schemaScripts,
                                            ScriptGeneratorManager context) {
        Collection<String> scripts = newArrayList();
        Dialect dialect = context.getTargetDialect();
        if (schemaScripts.size() == 1) {
            Map.Entry<Schema, Collection<String>> schemaScript = schemaScripts.entrySet().iterator().next();
            String useSpace = null;
            if (context.getTargetSchema() != null) {
                useSpace = dialect.getUseSchema(context.getTargetSchema(), true);
            } else if (context.getTargetCatalog() != null) {
                useSpace = dialect.getUseCatalog(context.getTargetCatalog(), true);
            }
            if (useSpace == null) {
                Schema schema = schemaScript.getKey();
                useSpace = schema.getIdentifier() != null ?
                        dialect.getUseSchema(context.getName(schema)) :
                        dialect.getUseCatalog(context.getName(schema.getCatalog()));
            }
            scripts.add(useSpace);
            scripts.addAll(schemaScript.getValue());
        } else {
            for (Map.Entry<Schema, Collection<String>> schemaScript : schemaScripts.entrySet()) {
                Schema schema = schemaScript.getKey();
                String useSpace = schema.getIdentifier() != null ?
                        dialect.getUseSchema(context.getName(schema)) :
                        dialect.getUseCatalog(context.getName(schema.getCatalog()));
                scripts.add(useSpace);
                scripts.addAll(schemaScript.getValue());
            }
        }
        return scripts;
    }

    protected Collection<Schema> getSchemas(HasSchemas hasSchemas, ScriptGeneratorManager context) {
        Collection<Schema> schemas = newArrayList();
        for (Schema schema : hasSchemas.getSchemas()) {
            if (addSchemaScripts(schema, context)) {
                schemas.add(schema);
            }
        }
        return schemas;
    }

    protected boolean addSchemaScripts(Schema schema, ScriptGeneratorManager context) {
        boolean generate = true;
        Identifier catalogId = valueOf(context.getSourceCatalog());
        if (catalogId != null) {
            generate = ObjectUtils.equals(catalogId, schema.getCatalog().getIdentifier());
        }
        Identifier schemaId = valueOf(context.getSourceSchema());
        if (generate && schemaId != null) {
            generate = ObjectUtils.equals(schemaId, schema.getIdentifier());
        }
        return generate;
    }
}
