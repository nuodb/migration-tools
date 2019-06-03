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
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.utils.StringUtils.isEmpty;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class ScriptGeneratorUtils {

    private static final String COMMA = ", ";

    public static Script getUseSchema(Schema schema, ScriptGeneratorManager scriptGeneratorManager) {
        String useSchema = null;
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        if (!isEmpty(scriptGeneratorManager.getTargetSchema())) {
            useSchema = dialect.getUseSchema(scriptGeneratorManager.getTargetSchema(), true);
        } else if (!isEmpty(scriptGeneratorManager.getTargetCatalog())) {
            useSchema = dialect.getUseSchema(scriptGeneratorManager.getTargetCatalog(), true);
        }
        if (useSchema == null) {
            useSchema = schema.getIdentifier() != null ? dialect.getUseSchema(scriptGeneratorManager.getName(schema))
                    : dialect.getUseSchema(scriptGeneratorManager.getName(schema.getCatalog()));
        }
        return new Script(useSchema);
    }

    public static Script getDropSchema(Schema schema, ScriptGeneratorManager scriptGeneratorManager) {
        String dropSchema = null;
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        if (scriptGeneratorManager.getTargetSchema() != null) {
            dropSchema = dialect.getDropSchema(scriptGeneratorManager.getTargetSchema(), true);
        } else if (scriptGeneratorManager.getTargetCatalog() != null) {
            dropSchema = dialect.getDropSchema(scriptGeneratorManager.getTargetCatalog(), true);
        }
        if (dropSchema == null) {
            dropSchema = schema.getIdentifier() != null ? dialect.getDropSchema(scriptGeneratorManager.getName(schema))
                    : dialect.getDropSchema(scriptGeneratorManager.getName(schema.getCatalog()));
        }
        return new Script(dropSchema);
    }

    public static Collection<Script> getCreateMultipleIndexes(Collection<Index> indexes,
            final ScriptGeneratorManager scriptGeneratorManager) {
        Collection<Script> multipleIndexesScripts = newArrayList();
        for (Index index : indexes) {
            Collection<Script> indexScripts = scriptGeneratorManager.getCreateScripts(index);
            if (size(indexScripts) > 0) {
                multipleIndexesScripts.add(get(indexScripts, 0));
            }

        }
        boolean requiresLock = false;
        Collection<String> sql = newArrayList();
        Table tableToLock = null;
        for (Script script : multipleIndexesScripts) {
            if (script.requiresLock()) {
                requiresLock = true;
                tableToLock = script.getTableToLock();
            }
            sql.add(script.getSQL());
        }
        return singleton(new Script(join(sql, COMMA), tableToLock, requiresLock));
    }
}
