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

import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.*;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class HasTablesScriptGenerator<H extends HasTables> extends ScriptGeneratorBase<H> {

    public static final String GROUP_SCRIPTS_BY = "GROUP_SCRIPTS_BY";

    public static final String TABLE_TYPES = "TABLE_TYPES";

    private static final String GENERATED_TABLES = "GENERATED_TABLES";

    private static final String PENDING_FOREIGN_KEYS = "PENDING_FOREIGN_KEYS";

    public HasTablesScriptGenerator() {
        this((Class<H>) HasTables.class);
    }

    protected HasTablesScriptGenerator(Class<H> objectClass) {
        super(objectClass);
    }

    @Override
    public Collection<String> getCreateScripts(H tables, ScriptGeneratorContext scriptGeneratorContext) {
        return getCreateHasTablesScripts(getTables(scriptGeneratorContext, tables), scriptGeneratorContext);
    }

    @Override
    public Collection<String> getDropScripts(H tables, ScriptGeneratorContext scriptGeneratorContext) {
        return getDropHasTablesScripts(getTables(scriptGeneratorContext, tables), scriptGeneratorContext);
    }

    @Override
    public Collection<String> getDropCreateScripts(H tables, ScriptGeneratorContext scriptGeneratorContext) {
        return getDropCreateHasTablesScripts(getTables(scriptGeneratorContext, tables), scriptGeneratorContext);
    }

    protected HasTables getTables(ScriptGeneratorContext scriptGeneratorContext, H tables) {
        return tables;
    }

    protected Collection<String> getCreateHasTablesScripts(HasTables tables,
                                                           ScriptGeneratorContext scriptGeneratorContext) {
        initScriptGeneratorContext(scriptGeneratorContext);
        Collection<String> scripts = newArrayList();
        GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorContext);
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : tables.getTables()) {
                    addCreateScripts(scriptGeneratorContext, scripts, singleton(table));
                }
                addForeignKeysScripts(scriptGeneratorContext, scripts, true);
                break;
            case META_DATA:
                addCreateScripts(scriptGeneratorContext, scripts, tables.getTables());
                break;
        }
        releaseScriptGeneratorContext(scriptGeneratorContext);
        return scripts;
    }

    protected Collection<String> getDropHasTablesScripts(HasTables tables,
                                                         ScriptGeneratorContext scriptGeneratorContext) {
        initScriptGeneratorContext(scriptGeneratorContext);
        Collection<String> scripts = newArrayList();
        GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorContext);
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : tables.getTables()) {
                    addDropScripts(scriptGeneratorContext, scripts, singleton(table));
                }
                break;
            case META_DATA:
                addCreateScripts(scriptGeneratorContext, scripts, tables.getTables());
                break;
        }
        releaseScriptGeneratorContext(scriptGeneratorContext);
        return scripts;
    }

    protected Collection<String> getDropCreateHasTablesScripts(HasTables tables,
                                                               ScriptGeneratorContext scriptGeneratorContext) {
        initScriptGeneratorContext(scriptGeneratorContext);
        Collection<String> scripts = newArrayList();
        GroupScriptsBy groupScriptsBy = (GroupScriptsBy) scriptGeneratorContext.getAttributes().get(GROUP_SCRIPTS_BY);
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : tables.getTables()) {
                    addDropScripts(scriptGeneratorContext, scripts, singleton(table));
                    addCreateScripts(scriptGeneratorContext, scripts, singleton(table));
                }
                addForeignKeysScripts(scriptGeneratorContext, scripts, true);
                break;
            case META_DATA:
                addDropScripts(scriptGeneratorContext, scripts, tables.getTables());
                addCreateScripts(scriptGeneratorContext, scripts, tables.getTables());
                break;
        }
        releaseScriptGeneratorContext(scriptGeneratorContext);
        return scripts;
    }

    protected GroupScriptsBy getGroupScriptsBy(ScriptGeneratorContext scriptGeneratorContext) {
        GroupScriptsBy groupScriptsBy = (GroupScriptsBy) scriptGeneratorContext.getAttributes().get(GROUP_SCRIPTS_BY);
        return groupScriptsBy != null ? GroupScriptsBy.TABLE : null;
    }

    protected void addCreateScripts(ScriptGeneratorContext scriptGeneratorContext, Collection<String> scripts,
                                    Collection<Table> tables) {
        Collection<MetaDataType> objectTypes = scriptGeneratorContext.getObjectTypes();
        Dialect dialect = scriptGeneratorContext.getDialect();
        if (objectTypes.contains(AUTO_INCREMENT)) {
            for (Table table : tables) {
                if (!shouldGenerateScript(scriptGeneratorContext, table)) {
                    continue;
                }
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(scriptGeneratorContext.getCreateScripts(column.getSequence()));
                    }
                }
            }
        }
        if (objectTypes.contains(TABLE)) {
            ScriptGeneratorContext tableScriptGeneratorContext = new ScriptGeneratorContext(scriptGeneratorContext);
            Collection<Table> generatedTables = (Collection<Table>)
                    tableScriptGeneratorContext.getAttributes().get(GENERATED_TABLES);
            tableScriptGeneratorContext.getObjectTypes().remove(FOREIGN_KEY);
            for (Table table : tables) {
                if (!shouldGenerateScript(scriptGeneratorContext, table)) {
                    continue;
                }
                generatedTables.add(table);
                scripts.addAll(tableScriptGeneratorContext.getCreateScripts(table));
            }
        }
        if (objectTypes.contains(INDEX) && !dialect.supportsIndexInCreateTable()) {
            for (Table table : tables) {
                boolean primary = false;
                for (Index index : table.getIndexes()) {
                    if (!primary && index.isPrimary()) {
                        primary = true;
                        continue;
                    }
                    if (!shouldGenerateScript(scriptGeneratorContext, index.getTable())) {
                        continue;
                    }
                    scripts.addAll(scriptGeneratorContext.getCreateScripts(index));
                }
            }
        }
        if (objectTypes.contains(FOREIGN_KEY)) {
            Collection<Table> processedTables =
                    (Collection<Table>) scriptGeneratorContext.getAttributes().get(GENERATED_TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) scriptGeneratorContext.getAttributes().get(PENDING_FOREIGN_KEYS);
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    Table targetTable = foreignKey.getForeignTable();
                    if (!shouldGenerateScript(scriptGeneratorContext, targetTable)) {
                        continue;
                    }
                    if (processedTables.contains(targetTable)) {
                        scripts.addAll(scriptGeneratorContext.getCreateScripts(foreignKey));
                    } else {
                        pendingForeignKeys.put(targetTable, foreignKey);
                    }
                }
            }
        }
        addForeignKeysScripts(scriptGeneratorContext, scripts, false);
    }

    protected void addForeignKeysScripts(ScriptGeneratorContext scriptGeneratorContext, Collection<String> scripts,
                                         boolean force) {
        Collection<MetaDataType> objectTypes = scriptGeneratorContext.getObjectTypes();
        if (objectTypes.contains(FOREIGN_KEY)) {
            Collection<Table> processedTables =
                    (Collection<Table>) scriptGeneratorContext.getAttributes().get(GENERATED_TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) scriptGeneratorContext.getAttributes().get(PENDING_FOREIGN_KEYS);
            for (ForeignKey foreignKey : newArrayList(pendingForeignKeys.values())) {
                Table table = foreignKey.getForeignTable();
                if (!shouldGenerateScript(scriptGeneratorContext, table)) {
                    continue;
                }
                if (processedTables.contains(table) || force) {
                    scripts.addAll(scriptGeneratorContext.getCreateScripts(foreignKey));
                    pendingForeignKeys.remove(table, foreignKey);
                }
            }
        }
    }

    protected void addDropScripts(ScriptGeneratorContext scriptGeneratorContext, Collection<String> scripts,
                                  Collection<Table> tables) {
        Collection<MetaDataType> objectTypes = scriptGeneratorContext.getObjectTypes();
        Dialect dialect = scriptGeneratorContext.getDialect();
        if (objectTypes.contains(FOREIGN_KEY) && dialect.supportsDropConstraints()) {
            for (Table table : tables) {
                if (!shouldGenerateScript(scriptGeneratorContext, table)) {
                    continue;
                }
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    scripts.addAll(scriptGeneratorContext.getDropScripts(foreignKey));
                }
            }
        }
        if (objectTypes.contains(TABLE)) {
            for (Table table : tables) {
                if (!shouldGenerateScript(scriptGeneratorContext, table)) {
                    continue;
                }
                scripts.addAll(scriptGeneratorContext.getDropScripts(table));
            }
        }
        if (objectTypes.contains(AUTO_INCREMENT) && dialect.supportsSequence()) {
            for (Table table : tables) {
                if (!shouldGenerateScript(scriptGeneratorContext, table)) {
                    continue;
                }
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(scriptGeneratorContext.getDropScripts(column.getSequence()));
                    }
                }
            }
        }
    }

    protected boolean shouldGenerateScript(ScriptGeneratorContext scriptGeneratorContext, Table table) {
        Collection<String> tableTypes = (Collection<String>) scriptGeneratorContext.getAttributes().get(TABLE_TYPES);
        return tableTypes != null ? tableTypes.contains(table.getType()) : Table.TABLE.equals(table.getType());
    }

    protected void initScriptGeneratorContext(ScriptGeneratorContext scriptGeneratorContext) {
        Map<String, Object> attributes = scriptGeneratorContext.getAttributes();
        attributes.put(GENERATED_TABLES, newLinkedHashSet());
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

    protected void releaseScriptGeneratorContext(ScriptGeneratorContext scriptGeneratorContext) {
        Map<String, Object> attributes = scriptGeneratorContext.getAttributes();
        attributes.remove(GENERATED_TABLES);
        attributes.remove(PENDING_FOREIGN_KEYS);
    }
}
