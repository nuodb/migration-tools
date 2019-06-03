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
import com.nuodb.migrator.jdbc.metadata.HasSchemas;
import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.Schema;

import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorUtils.getDropSchema;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorUtils.getUseSchema;

/**
 * @author Sergey Bushik
 */
public class HasSchemasScriptGenerator extends HasTablesScriptGenerator<HasSchemas> {

    public HasSchemasScriptGenerator() {
        super(HasSchemas.class);
    }

    @Override
    public Collection<Script> getScripts(HasSchemas hasSchemas, ScriptGeneratorManager scriptGeneratorManager) {
        Map<Schema, Collection<Script>> schemaScripts = newLinkedHashMap();
        for (Schema schema : getSchemas(hasSchemas, scriptGeneratorManager)) {
            Collection<Script> scripts = getScripts(schema, scriptGeneratorManager);
            if (!scripts.isEmpty()) {
                schemaScripts.put(schema, scripts);
            }
        }
        return getScripts(schemaScripts, scriptGeneratorManager, false, true);
    }

    protected Collection<Script> getScripts(Map<Schema, Collection<Script>> schemaScripts,
            ScriptGeneratorManager scriptGeneratorManager, boolean dropSchema, boolean useSchema) {
        Collection<Script> scripts = newArrayList();
        if (schemaScripts.size() == 1) {
            Map.Entry<Schema, Collection<Script>> schemaScript = schemaScripts.entrySet().iterator().next();
            Schema schema = schemaScript.getKey();
            if (dropSchema) {
                scripts.add(getDropSchema(schema, scriptGeneratorManager));
            }
            if (useSchema) {
                scripts.add(getUseSchema(schema, scriptGeneratorManager));
            }
            scripts.addAll(schemaScript.getValue());
        } else {
            for (Map.Entry<Schema, Collection<Script>> schemaScript : schemaScripts.entrySet()) {
                Schema schema = schemaScript.getKey();
                Dialect dialect = scriptGeneratorManager.getTargetDialect();
                if (dropSchema) {
                    scripts.add(getDropSchema(schema, scriptGeneratorManager));
                }
                if (useSchema) {
                    scripts.add(schema.getIdentifier() != null
                            ? new Script(dialect.getUseSchema(scriptGeneratorManager.getName(schema)))
                            : new Script(dialect.getUseSchema(scriptGeneratorManager.getName(schema.getCatalog()))));
                }
                scripts.addAll(schemaScript.getValue());
            }
        }
        return scripts;
    }

    protected Collection<Schema> getSchemas(HasSchemas hasSchemas, ScriptGeneratorManager scriptGeneratorManager) {
        Collection<Schema> schemas = newArrayList();
        for (Schema schema : hasSchemas.getSchemas()) {
            if (addScripts(schema, scriptGeneratorManager)) {
                schemas.add(schema);
            }
        }
        return schemas;
    }

    protected boolean addScripts(Schema schema, ScriptGeneratorManager manager) {
        boolean generate = true;
        Identifier catalogId = valueOf(manager.getSourceCatalog());
        if (catalogId != null) {
            generate = ObjectUtils.equals(catalogId, schema.getCatalog().getIdentifier());
        }
        Identifier schemaId = valueOf(manager.getSourceSchema());
        if (generate && schemaId != null) {
            generate = ObjectUtils.equals(schemaId, schema.getIdentifier());
        }
        return generate;
    }
}
