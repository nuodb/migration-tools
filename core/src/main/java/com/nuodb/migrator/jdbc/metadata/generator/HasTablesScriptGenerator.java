/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.HasTables;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.Trigger;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class HasTablesScriptGenerator<H extends HasTables> extends ScriptGeneratorBase<H> {

    public static final String GROUP_SCRIPTS_BY = "group.scripts.by";

    public static final String TABLE_TYPES = "table.types";

    private static final String TABLES = "tables";

    private static final String FOREIGN_KEYS = "foreign.keys";

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

    protected Collection<String> getHasTablesCreateScripts(HasTables tables,
                                                           ScriptGeneratorManager scriptGeneratorManager) {
        initScriptGeneratorContext(scriptGeneratorManager);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorManager);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addTablesCreateScripts(singleton(table), scripts, scriptGeneratorManager);
                    }
                    addForeignKeysScripts(scripts, true, scriptGeneratorManager);
                    break;
                case META_DATA:
                    addTablesCreateScripts(tables.getTables(), scripts, scriptGeneratorManager);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(scriptGeneratorManager);
        }
    }

    protected Collection<String> getHasTablesDropScripts(HasTables tables,
                                                         ScriptGeneratorManager scriptGeneratorManager) {
        initScriptGeneratorContext(scriptGeneratorManager);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorManager);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addTablesDropScripts(singleton(table), scripts, scriptGeneratorManager);
                    }
                    break;
                case META_DATA:
                    addTablesDropScripts(tables.getTables(), scripts, scriptGeneratorManager);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(scriptGeneratorManager);
        }
    }

    protected Collection<String> getHasTablesDropCreateScripts(HasTables tables,
                                                               ScriptGeneratorManager scriptGeneratorManager) {
        initScriptGeneratorContext(scriptGeneratorManager);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorManager);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addTablesDropScripts(singleton(table), scripts, scriptGeneratorManager);
                        addTablesCreateScripts(singleton(table), scripts, scriptGeneratorManager);
                    }
                    addForeignKeysScripts(scripts, true, scriptGeneratorManager);
                    break;
                case META_DATA:
                    addTablesDropScripts(tables.getTables(), scripts, scriptGeneratorManager);
                    addTablesCreateScripts(tables.getTables(), scripts, scriptGeneratorManager);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(scriptGeneratorManager);
        }
    }

    protected GroupScriptsBy getGroupScriptsBy(ScriptGeneratorManager scriptGeneratorManager) {
        GroupScriptsBy groupScriptsBy = (GroupScriptsBy) scriptGeneratorManager.getAttribute(GROUP_SCRIPTS_BY);
        return groupScriptsBy != null ? groupScriptsBy : GroupScriptsBy.TABLE;
    }

    protected void addTablesCreateScripts(Collection<Table> tables, Collection<String> scripts,
                                          ScriptGeneratorManager scriptGeneratorManager) {
        Collection<MetaDataType> objectTypes = scriptGeneratorManager.getObjectTypes();
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        boolean createSequences = scriptGeneratorManager.getObjectTypes().contains(SEQUENCE) &&
                scriptGeneratorManager.getTargetDialect().supportsSequence();
        boolean createTables = objectTypes.contains(TABLE);
        boolean createIndexes = objectTypes.contains(INDEX);
        boolean createPrimaryKeys = objectTypes.contains(PRIMARY_KEY);
        boolean createForeignKeys = objectTypes.contains(FOREIGN_KEY);
        boolean createTriggers = objectTypes.contains(TRIGGER);
        boolean createColumnTriggers = objectTypes.contains(COLUMN_TRIGGER);
        if (createSequences) {
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                for (Sequence sequence : table.getSequences()) {
                    scripts.addAll(scriptGeneratorManager.getCreateScripts(sequence));
                }
            }
        }
        if (createTables) {
            ScriptGeneratorManager tableScriptGeneratorManager = new ScriptGeneratorManager(scriptGeneratorManager);
            Collection<Table> primaryTables = (Collection<Table>)
                    tableScriptGeneratorManager.getAttributes().get(TABLES);
            tableScriptGeneratorManager.getObjectTypes().remove(FOREIGN_KEY);
            for (Table table : tables) {
                if (!addTableScripts(table, scriptGeneratorManager)) {
                    continue;
                }
                scripts.addAll(tableScriptGeneratorManager.getCreateScripts(table));
                primaryTables.add(table);
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
            Collection<Table> primaryTables = (Collection<Table>) scriptGeneratorManager.getAttribute(TABLES);
            Multimap<Table, ForeignKey> foreignKeys =
                    (Multimap<Table, ForeignKey>) scriptGeneratorManager.getAttribute(FOREIGN_KEYS);
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    Table primaryTable = foreignKey.getPrimaryTable();
                    Table foreignTable = foreignKey.getForeignTable();
                    if (!addTableScripts(primaryTable, scriptGeneratorManager) ||
                            !addTableScripts(foreignTable, scriptGeneratorManager)) {
                        continue;
                    }
                    if (!primaryTables.contains(primaryTable)) {
                        foreignKeys.put(primaryTable, foreignKey);
                    } else {
                        foreignKeys.remove(primaryTable, foreignKey);
                        scripts.addAll(scriptGeneratorManager.getCreateScripts(foreignKey));
                    }
                }
            }
        }
        addForeignKeysScripts(scripts, false, scriptGeneratorManager);
    }

    protected void addForeignKeysScripts(Collection<String> scripts, boolean force,
                                         ScriptGeneratorManager scriptGeneratorManager) {
        Collection<MetaDataType> objectTypes = scriptGeneratorManager.getObjectTypes();
        if (objectTypes.contains(FOREIGN_KEY)) {
            Collection<Table> primaryTables = (Collection<Table>) scriptGeneratorManager.getAttribute(TABLES);
            Multimap<Table, ForeignKey> foreignKeys =
                    (Multimap<Table, ForeignKey>) scriptGeneratorManager.getAttribute(FOREIGN_KEYS);
            for (ForeignKey foreignKey : newArrayList(foreignKeys.values())) {
                Table primaryTable = foreignKey.getPrimaryTable();
                if (!addTableScripts(primaryTable, scriptGeneratorManager) ||
                        !addTableScripts(foreignKey.getForeignTable(), scriptGeneratorManager)) {
                    continue;
                }
                if (primaryTables.contains(primaryTable) || force) {
                    scripts.addAll(scriptGeneratorManager.getCreateScripts(foreignKey));
                    foreignKeys.remove(primaryTable, foreignKey);
                }
            }
        }
    }

    protected void addTablesDropScripts(Collection<Table> tables, Collection<String> scripts,
                                        ScriptGeneratorManager scriptGeneratorManager) {
        Collection<MetaDataType> objectTypes = scriptGeneratorManager.getObjectTypes();
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        boolean dropForeignKeys = objectTypes.contains(FOREIGN_KEY) &&
                dialect.supportsDropConstraints();
        boolean dropTriggers = objectTypes.contains(TRIGGER);
        boolean dropColumnTriggers = objectTypes.contains(COLUMN_TRIGGER);
        boolean dropTables = objectTypes.contains(TABLE);
        boolean dropSequences = objectTypes.contains(SEQUENCE) &&
                scriptGeneratorManager.getTargetDialect().supportsSequence();
        for (Table table : tables) {
            if (!addTableScripts(table, scriptGeneratorManager)) {
                continue;
            }
            if (dropForeignKeys) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    scripts.addAll(scriptGeneratorManager.getDropScripts(foreignKey));
                }
            }
        }
        for (Table table : tables) {
            if (!addTableScripts(table, scriptGeneratorManager)) {
                continue;
            }
            Collection<String> triggers = newLinkedHashSet();
            for (Trigger trigger : table.getTriggers()) {
                if (trigger.getObjectType() == TRIGGER && dropTriggers) {
                    triggers.addAll(scriptGeneratorManager.getDropScripts(trigger));
                } else if (trigger.getObjectType() == COLUMN_TRIGGER && dropColumnTriggers) {
                    triggers.addAll(scriptGeneratorManager.getDropScripts(trigger));
                }
            }
            scripts.addAll(triggers);
        }
        for (Table table : tables) {
            if (!addTableScripts(table, scriptGeneratorManager)) {
                continue;
            }
            if (dropTables) {
                scripts.addAll(scriptGeneratorManager.getDropScripts(table));
            }
        }
        for (Table table : tables) {
            if (!addTableScripts(table, scriptGeneratorManager)) {
                continue;
            }
            if (dropSequences) {
                for (Sequence sequence : table.getSequences()) {
                    scripts.addAll(scriptGeneratorManager.getDropScripts(sequence));
                }
            }
        }
    }

    protected boolean addTableScripts(Table table, ScriptGeneratorManager scriptGeneratorManager) {
        Collection<String> tableTypes = (Collection<String>) scriptGeneratorManager.getAttributes().get(TABLE_TYPES);
        return tableTypes != null ? tableTypes.contains(table.getType()) : Table.TABLE.equals(table.getType());
    }

    protected void initScriptGeneratorContext(ScriptGeneratorManager scriptGeneratorManager) {
        scriptGeneratorManager.addAttribute(TABLES, newLinkedHashSet());
        scriptGeneratorManager.addAttribute(FOREIGN_KEYS, HashMultimap.create());
    }

    protected void releaseScriptGeneratorContext(ScriptGeneratorManager scriptGeneratorManager) {
        scriptGeneratorManager.removeAttribute(TABLES);
        scriptGeneratorManager.removeAttribute(FOREIGN_KEYS);
    }
}
