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

import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.*;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migration.jdbc.metadata.MetaDataType.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DatabaseScriptGenerator extends ScriptGeneratorBase<Database> {

    public static final String GROUP_SCRIPTS_BY = "GROUP_SCRIPTS_BY";

    public static final String TABLE_TYPES = "TABLE_TYPES";

    private static final String PROCESSED_TABLES = "PROCESSED_TABLES";

    private static final String PENDING_FOREIGN_KEYS = "PENDING_FOREIGN_KEYS";

    public DatabaseScriptGenerator() {
        super(Database.class);
    }

    @Override
    public Collection<String> getCreateScripts(Database database, ScriptGeneratorContext scriptGeneratorContext) {
        initScriptGeneratorContext(scriptGeneratorContext);
        Collection<String> scripts = newArrayList();
        GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorContext);
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : database.getTables()) {
                    Collection<Table> tables = Collections.singleton(table);
                    addCreateScripts(scriptGeneratorContext, scripts, tables);
                }
                addForeignKeysScripts(scriptGeneratorContext, scripts, true);
                break;
            case META_DATA:
                Collection<Table> tables = database.getTables();
                addCreateScripts(scriptGeneratorContext, scripts, tables);
                break;
        }
        releaseScriptGeneratorContext(scriptGeneratorContext);
        return scripts;
    }

    @Override
    public Collection<String> getDropScripts(Database database, ScriptGeneratorContext scriptGeneratorContext) {
        initScriptGeneratorContext(scriptGeneratorContext);
        Collection<String> scripts = newArrayList();
        GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorContext);
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : database.getTables()) {
                    Collection<Table> tables = Collections.singleton(table);
                    addDropScripts(scriptGeneratorContext, scripts, tables);
                }
                break;
            case META_DATA:
                Collection<Table> tables = database.getTables();
                addCreateScripts(scriptGeneratorContext, scripts, tables);
                break;
        }
        releaseScriptGeneratorContext(scriptGeneratorContext);
        return scripts;
    }

    @Override
    public Collection<String> getDropCreateScripts(Database database, ScriptGeneratorContext scriptGeneratorContext) {
        initScriptGeneratorContext(scriptGeneratorContext);
        Collection<String> scripts = newArrayList();
        GroupScriptsBy groupScriptsBy = (GroupScriptsBy) scriptGeneratorContext.getAttributes().get(GROUP_SCRIPTS_BY);
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : database.getTables()) {
                    Collection<Table> tables = Collections.singleton(table);
                    addDropScripts(scriptGeneratorContext, scripts, tables);
                    addCreateScripts(scriptGeneratorContext, scripts, tables);
                }
                addForeignKeysScripts(scriptGeneratorContext, scripts, true);
                break;
            case META_DATA:
                Collection<Table> tables = database.getTables();
                addDropScripts(scriptGeneratorContext, scripts, tables);
                addCreateScripts(scriptGeneratorContext, scripts, tables);
                break;
        }
        releaseScriptGeneratorContext(scriptGeneratorContext);
        return scripts;
    }

    protected void addCreateScripts(ScriptGeneratorContext context, Collection<String> scripts,
                                    Collection<Table> tables) {
        Collection<MetaDataType> metaDataTypes = context.getMetaDataTypes();
        Dialect dialect = context.getDialect();
        if (metaDataTypes.contains(AUTO_INCREMENT)) {
            for (Table table : tables) {
                if (!isTableSupported(table, context)) {
                    continue;
                }
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(context.getCreateScripts(column.getSequence()));
                    }
                }
            }
        }
        if (metaDataTypes.contains(TABLE)) {
            ScriptGeneratorContext tableGeneratorContext = new ScriptGeneratorContext(context);
            Collection<Table> processedTables = (Collection<Table>)
                    tableGeneratorContext.getAttributes().get(PROCESSED_TABLES);
            tableGeneratorContext.getMetaDataTypes().remove(FOREIGN_KEY);
            for (Table table : tables) {
                if (!isTableSupported(table, context)) {
                    continue;
                }
                processedTables.add(table);
                scripts.addAll(tableGeneratorContext.getCreateScripts(table));
            }
        }
        if (metaDataTypes.contains(INDEX) && !dialect.supportsIndexInCreateTable()) {
            for (Table table : tables) {
                boolean primary = false;
                for (Index index : table.getIndexes()) {
                    if (!primary && index.isPrimary()) {
                        primary = true;
                        continue;
                    }
                    if (!isTableSupported(index.getTable(), context)) {
                        continue;
                    }
                    scripts.addAll(context.getCreateScripts(index));
                }
            }
        }
        if (metaDataTypes.contains(FOREIGN_KEY)) {
            Collection<Table> processedTables =
                    (Collection<Table>) context.getAttributes().get(PROCESSED_TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) context.getAttributes().get(PENDING_FOREIGN_KEYS);
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    Table targetTable = foreignKey.getForeignTable();
                    if (!isTableSupported(targetTable, context)) {
                        continue;
                    }
                    if (processedTables.contains(targetTable)) {
                        scripts.addAll(context.getCreateScripts(foreignKey));
                    } else {
                        pendingForeignKeys.put(targetTable, foreignKey);
                    }
                }
            }
        }
        addForeignKeysScripts(context, scripts, false);
    }

    protected void addForeignKeysScripts(ScriptGeneratorContext context,
                                         Collection<String> scripts, boolean force) {
        Collection<MetaDataType> metaDataTypes = context.getMetaDataTypes();
        if (metaDataTypes.contains(FOREIGN_KEY)) {
            Collection<Table> processedTables =
                    (Collection<Table>) context.getAttributes().get(PROCESSED_TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) context.getAttributes().get(PENDING_FOREIGN_KEYS);
            for (ForeignKey foreignKey : newArrayList(pendingForeignKeys.values())) {
                Table table = foreignKey.getForeignTable();
                if (!isTableSupported(table, context)) {
                    continue;
                }
                if (processedTables.contains(table) || force) {
                    scripts.addAll(context.getCreateScripts(foreignKey));
                    pendingForeignKeys.remove(table, foreignKey);
                }
            }
        }
    }

    protected void addDropScripts(ScriptGeneratorContext context,
                                  Collection<String> scripts, Collection<Table> tables) {
        Collection<MetaDataType> metaDataTypes = context.getMetaDataTypes();
        Dialect dialect = context.getDialect();
        if (metaDataTypes.contains(FOREIGN_KEY) && dialect.supportsDropConstraints()) {
            for (Table table : tables) {
                if (!isTableSupported(table, context)) {
                    continue;
                }
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    scripts.addAll(context.getDropScripts(foreignKey));
                }
            }
        }
        if (metaDataTypes.contains(TABLE)) {
            for (Table table : tables) {
                if (!isTableSupported(table, context)) {
                    continue;
                }
                scripts.addAll(context.getDropScripts(table));
            }
        }
        if (metaDataTypes.contains(AUTO_INCREMENT) && dialect.supportsSequence()) {
            for (Table table : tables) {
                if (!isTableSupported(table, context)) {
                    continue;
                }
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(context.getDropScripts(column.getSequence()));
                    }
                }
            }
        }
    }

    protected GroupScriptsBy getGroupScriptsBy(ScriptGeneratorContext context) {
        GroupScriptsBy groupScriptsBy = (GroupScriptsBy) context.getAttributes().get(GROUP_SCRIPTS_BY);
        return groupScriptsBy != null ? GroupScriptsBy.TABLE : null;
    }

    protected boolean isTableSupported(Table table, ScriptGeneratorContext context) {
        Collection<String> tableTypes = (Collection<String>) context.getAttributes().get(TABLE_TYPES);
        return tableTypes != null ? tableTypes.contains(table.getType()) : Table.TABLE.equals(table.getType());
    }

    protected void initScriptGeneratorContext(ScriptGeneratorContext context) {
        Map<String, Object> attributes = context.getAttributes();
        attributes.put(PROCESSED_TABLES, newLinkedHashSet());
        attributes.put(PENDING_FOREIGN_KEYS,
                Multimaps.<Table, ForeignKey>newSetMultimap(new HashMap<Table, Collection<ForeignKey>>(),
                        new Supplier<Set<ForeignKey>>() {
                            @Override
                            public Set<ForeignKey> get() {
                                return new HashSet<ForeignKey>();
                            }
                        })
        );
    }

    protected void releaseScriptGeneratorContext(ScriptGeneratorContext context) {
        Map<String, Object> attributes = context.getAttributes();
        attributes.remove(PROCESSED_TABLES);
        attributes.remove(PENDING_FOREIGN_KEYS);
    }
}
