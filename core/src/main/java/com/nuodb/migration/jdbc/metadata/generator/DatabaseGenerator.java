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
import com.nuodb.migration.jdbc.metadata.*;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DatabaseGenerator implements ScriptGenerator<Database> {

    @Override
    public Class<Database> getObjectType() {
        return Database.class;
    }

    @Override
    public String[] getCreateScripts(Database database, ScriptGeneratorContext context) {
        List<String> scripts = newArrayList();
        Collection<Table> tables = database.listTables();
        /**
         * Indexes and foreign keys are generated after tables
         */
        Collection<MetaDataType> metaDataTypes = newArrayList(MetaDataType.ALL_TYPES);
        metaDataTypes.remove(MetaDataType.FOREIGN_KEY);
        SimpleScriptGeneratorContext tableContext = new SimpleScriptGeneratorContext(context);
        tableContext.setMetaDataTypes(metaDataTypes);
        for (Table table : tables) {
            scripts.addAll(newArrayList(tableContext.getCreateScripts(table)));
        }
        for (Table table : tables) {
            if (!context.getDialect().supportsIndexInCreateTable()) {
                boolean primary = false;
                for (Index index : table.getIndexes()) {
                    if (!primary && index.isPrimary()) {
                        primary = true;
                        continue;
                    }
                    scripts.addAll(newArrayList(tableContext.getCreateScripts(index)));
                }
            }
            for (ForeignKey foreignKey : table.getForeignKeys()) {
                scripts.addAll(newArrayList(tableContext.getCreateScripts(foreignKey)));
            }
        }
        return scripts.toArray(new String[scripts.size()]);
    }

    @Override
    public String[] getDropScripts(Database database, ScriptGeneratorContext context) {
        List<String> scripts = newArrayList();
        Dialect dialect = context.getDialect();
        Collection<Table> tables = database.listTables();
        if (dialect.dropConstraints()) {
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    scripts.addAll(newArrayList(context.getDropScripts(foreignKey)));
                }
            }
        }
        for (Table table : tables) {
            scripts.addAll(newArrayList(context.getDropScripts(table)));
        }
        return scripts.toArray(new String[scripts.size()]);
    }
}
