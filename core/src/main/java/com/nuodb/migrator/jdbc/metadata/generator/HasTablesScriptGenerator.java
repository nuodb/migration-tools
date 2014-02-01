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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    private static final String TABLES = "TABLES";

    private static final String FOREIGN_KEYS = "FOREIGN_KEYS";

    public HasTablesScriptGenerator() {
        this((Class<H>) HasTables.class);
    }

    protected HasTablesScriptGenerator(Class<H> objectClass) {
        super(objectClass);
    }

    @Override
    public Collection<String> getCreateScripts(H tables, ScriptGeneratorManager scriptGeneratorManager) {
        return getHasTablesCreateScripts(getTables(tables, scriptGeneratorManager), scriptGeneratorManager);
    }

    @Override
    public Collection<String> getDropScripts(H tables, ScriptGeneratorManager scriptGeneratorManager) {
        return getHasTablesDropScripts(getTables(tables, scriptGeneratorManager), scriptGeneratorManager);
    }

    @Override
    public Collection<String> getDropCreateScripts(H tables, ScriptGeneratorManager scriptGeneratorManager) {
        return getHasTablesDropCreateScripts(getTables(tables, scriptGeneratorManager), scriptGeneratorManager);
    }

    protected HasTables getTables(H tables, ScriptGeneratorManager scriptGeneratorManager) {
        return tables;
    }

    protected Collection<String> getHasTablesCreateScripts(HasTables tables, ScriptGeneratorManager scriptGeneratorManager) {
        initScriptGeneratorContext(scriptGeneratorManager);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorManager);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addCreateScripts(singleton(table), scripts, scriptGeneratorManager);
                    }
                    addForeignKeysScripts(scripts, true, scriptGeneratorManager);
                    break;
                case META_DATA:
                    addCreateScripts(tables.getTables(), scripts, scriptGeneratorManager);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(scriptGeneratorManager);
        }
    }

    protected Collection<String> getHasTablesDropScripts(HasTables tables, ScriptGeneratorManager scriptGeneratorManager) {
        initScriptGeneratorContext(scriptGeneratorManager);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorManager);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addDropScripts(singleton(table), scripts, scriptGeneratorManager);
                    }
                    break;
                case META_DATA:
                    addCreateScripts(tables.getTables(), scripts, scriptGeneratorManager);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(scriptGeneratorManager);
        }
    }

    protected Collection<String> getHasTablesDropCreateScripts(HasTables tables, ScriptGeneratorManager scriptGeneratorManager) {
        initScriptGeneratorContext(scriptGeneratorManager);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = (GroupScriptsBy) scriptGeneratorManager.getAttributes().get(GROUP_SCRIPTS_BY);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addDropScripts(singleton(table), scripts, scriptGeneratorManager);
                        addCreateScripts(singleton(table), scripts, scriptGeneratorManager);
                    }
                    addForeignKeysScripts(scripts, true, scriptGeneratorManager);
                    break;
                case META_DATA:
                    addDropScripts(tables.getTables(), scripts, scriptGeneratorManager);
                    addCreateScripts(tables.getTables(), scripts, scriptGeneratorManager);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(scriptGeneratorManager);
        }
    }

    protected GroupScriptsBy getGroupScriptsBy(ScriptGeneratorManager scriptGeneratorManager) {
        GroupScriptsBy groupScriptsBy = (GroupScriptsBy) scriptGeneratorManager.getAttributes().get(GROUP_SCRIPTS_BY);
        return groupScriptsBy != null ? groupScriptsBy : GroupScriptsBy.TABLE;
    }

    protected void addCreateScripts(Collection<Table> tables, Collection<String> scripts,
                                    ScriptGeneratorManager scriptGeneratorManager) {
        Collection<MetaDataType> objectTypes = scriptGeneratorManager.getObjectTypes();
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        boolean createTables = objectTypes.contains(TABLE);
        boolean createIndexes = objectTypes.contains(INDEX);
        boolean createPrimaryKeys = objectTypes.contains(PRIMARY_KEY);
        boolean createIdentities = objectTypes.contains(SEQUENCE);
        boolean createForeignKeys = objectTypes.contains(FOREIGN_KEY);
        boolean createTriggers = objectTypes.contains(TRIGGER);
        boolean createColumnTriggers = objectTypes.contains(COLUMN_TRIGGER);
        if (createIdentities) {
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(scriptGeneratorManager.getCreateScripts(column.getSequence()));
                    }
                }
            }
        }
        if (createTables) {
            ScriptGeneratorManager tableScriptGeneratorManager = new ScriptGeneratorManager(scriptGeneratorManager);
            Collection<Table> tableFromContext = (Collection<Table>)
                    tableScriptGeneratorManager.getAttributes().get(TABLES);
            tableScriptGeneratorManager.getObjectTypes().remove(FOREIGN_KEY);
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                scripts.addAll(tableScriptGeneratorManager.getCreateScripts(table));
                tableFromContext.add(table);
            }
        }
        if (createPrimaryKeys && !createTables) {
            Collection<String> primaryKeys = newLinkedHashSet();
            for (Table table : tables) {
                PrimaryKey primaryKey = table.getPrimaryKey();
                if (primaryKey != null) {
                    primaryKeys.addAll(scriptGeneratorManager.getCreateScripts(primaryKey));
                }
            }
            scripts.addAll(primaryKeys);
        }
        if (createIndexes && (!dialect.supportsIndexInCreateTable() || !createTables)) {
            Collection<String> indexes = newLinkedHashSet();
            for (Table table : tables) {
                for (Index index : table.getIndexes()) {
                    if (!addTableScripts(index.getTable(), scriptGeneratorManager) || index.isPrimary()) {
                        continue;
                    }
                    indexes.addAll(scriptGeneratorManager.getCreateScripts(index));
                }
            }
            scripts.addAll(indexes);
        }
        if (createTriggers || createColumnTriggers) {
            Collection<String> triggers = newLinkedHashSet();
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                for (Trigger trigger : table.getTriggers()) {
                    if (trigger.getObjectType() == TRIGGER && createTriggers) {
                        triggers.addAll(scriptGeneratorManager.getCreateScripts(trigger));
                    } else if (trigger.getObjectType() == COLUMN_TRIGGER && createColumnTriggers) {
                        triggers.addAll(scriptGeneratorManager.getCreateScripts(trigger));
                    }
                }
            }
            scripts.addAll(triggers);
        }

        if (createForeignKeys) {
            Collection<Table> generatedTables = (Collection<Table>) scriptGeneratorManager.getAttributes().get(TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) scriptGeneratorManager.getAttributes().get(FOREIGN_KEYS);
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    Table primaryTable = foreignKey.getPrimaryTable();
                    if (!addTableScripts(primaryTable, scriptGeneratorManager) ||
                            !addTableScripts(foreignKey.getForeignTable(), scriptGeneratorManager)) {
                        continue;
                    }
                    if (generatedTables.contains(primaryTable)) {
                        scripts.addAll(scriptGeneratorManager.getCreateScripts(foreignKey));
                    } else {
                        pendingForeignKeys.put(primaryTable, foreignKey);
                    }
                }
            }
        }
        addForeignKeysScripts(scripts, false, scriptGeneratorManager);
    }

    protected void addForeignKeysScripts(Collection<String> scripts, boolean force, ScriptGeneratorManager scriptGeneratorManager) {
        Collection<MetaDataType> objectTypes = scriptGeneratorManager.getObjectTypes();
        if (objectTypes.contains(FOREIGN_KEY)) {
            Collection<Table> generatedTables = (Collection<Table>) scriptGeneratorManager.getAttributes().get(TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) scriptGeneratorManager.getAttributes().get(FOREIGN_KEYS);
            for (ForeignKey foreignKey : newArrayList(pendingForeignKeys.values())) {
                Table primaryTable = foreignKey.getPrimaryTable();
                if (!addTableScripts(primaryTable, scriptGeneratorManager) ||
                        !addTableScripts(foreignKey.getForeignTable(), scriptGeneratorManager)) {
                    continue;
                }
                if (generatedTables.contains(primaryTable) || force) {
                    scripts.addAll(scriptGeneratorManager.getCreateScripts(foreignKey));
                    pendingForeignKeys.remove(primaryTable, foreignKey);
                }
            }
        }
    }

    protected void addDropScripts(Collection<Table> tables, Collection<String> scripts,
                                  ScriptGeneratorManager scriptGeneratorManager) {
        Collection<MetaDataType> objectTypes = scriptGeneratorManager.getObjectTypes();
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        if (objectTypes.contains(FOREIGN_KEY) && dialect.supportsDropConstraints()) {
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    scripts.addAll(scriptGeneratorManager.getDropScripts(foreignKey));
                }
            }
        }
        boolean dropTriggers = objectTypes.contains(TRIGGER);
        boolean dropColumnTriggers = objectTypes.contains(COLUMN_TRIGGER);
        if (dropTriggers || dropColumnTriggers) {
            Collection<String> triggers = newLinkedHashSet();
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                for (Trigger trigger : table.getTriggers()) {
                    if (trigger.getObjectType() == TRIGGER && dropTriggers) {
                        triggers.addAll(scriptGeneratorManager.getDropScripts(trigger));
                    } else if (trigger.getObjectType() == COLUMN_TRIGGER && dropColumnTriggers) {
                        triggers.addAll(scriptGeneratorManager.getDropScripts(trigger));
                    }
                }
            }
            scripts.addAll(triggers);
        }
        if (objectTypes.contains(TABLE)) {
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                scripts.addAll(scriptGeneratorManager.getDropScripts(table));
            }
        }
        if (objectTypes.contains(SEQUENCE) && dialect.supportsSequence()) {
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(scriptGeneratorManager.getDropScripts(column.getSequence()));
                    }
                }
            }
        }
    }

    protected boolean addTableScripts(Table table, ScriptGeneratorManager scriptGeneratorManager) {
        Collection<String> tableTypes = (Collection<String>) scriptGeneratorManager.getAttributes().get(TABLE_TYPES);
        return tableTypes != null ? tableTypes.contains(table.getType()) : Table.TABLE.equals(table.getType());
    }

    protected void initScriptGeneratorContext(ScriptGeneratorManager scriptGeneratorManager) {
        Map<String, Object> attributes = scriptGeneratorManager.getAttributes();
        attributes.put(TABLES, newLinkedHashSet());
        attributes.put(FOREIGN_KEYS,
                Multimaps.<Table, ForeignKey>newSetMultimap(new HashMap<Table, Collection<ForeignKey>>(),
                        new Supplier<Set<ForeignKey>>() {
                            @Override
                            public Set<ForeignKey> get() {
                                return new HashSet<ForeignKey>();
                            }
                        })
        );
    }

    protected void releaseScriptGeneratorContext(ScriptGeneratorManager scriptGeneratorManager) {
        Map<String, Object> attributes = scriptGeneratorManager.getAttributes();
        attributes.remove(TABLES);
        attributes.remove(FOREIGN_KEYS);
    }
}
