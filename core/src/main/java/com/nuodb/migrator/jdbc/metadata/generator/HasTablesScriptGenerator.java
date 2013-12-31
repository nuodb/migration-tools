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

    protected Collection<String> getHasTablesCreateScripts(HasTables tables, ScriptGeneratorManager context) {
        initScriptGeneratorContext(context);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(context);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addCreateScripts(singleton(table), scripts, context);
                    }
                    addForeignKeysScripts(scripts, true, context);
                    break;
                case META_DATA:
                    addCreateScripts(tables.getTables(), scripts, context);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(context);
        }
    }

    protected Collection<String> getHasTablesDropScripts(HasTables tables, ScriptGeneratorManager context) {
        initScriptGeneratorContext(context);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(context);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addDropScripts(singleton(table), scripts, context);
                    }
                    break;
                case META_DATA:
                    addCreateScripts(tables.getTables(), scripts, context);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(context);
        }
    }

    protected Collection<String> getHasTablesDropCreateScripts(HasTables tables, ScriptGeneratorManager context) {
        initScriptGeneratorContext(context);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = (GroupScriptsBy) context.getAttributes().get(GROUP_SCRIPTS_BY);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addDropScripts(singleton(table), scripts, context);
                        addCreateScripts(singleton(table), scripts, context);
                    }
                    addForeignKeysScripts(scripts, true, context);
                    break;
                case META_DATA:
                    addDropScripts(tables.getTables(), scripts, context);
                    addCreateScripts(tables.getTables(), scripts, context);
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(context);
        }
    }

    protected GroupScriptsBy getGroupScriptsBy(ScriptGeneratorManager context) {
        GroupScriptsBy groupScriptsBy = (GroupScriptsBy) context.getAttributes().get(GROUP_SCRIPTS_BY);
        return groupScriptsBy != null ? groupScriptsBy : GroupScriptsBy.TABLE;
    }

    protected void addCreateScripts(Collection<Table> tables, Collection<String> scripts,
                                    ScriptGeneratorManager context) {
        Collection<MetaDataType> objectTypes = context.getObjectTypes();
        Dialect dialect = context.getTargetDialect();
        if (objectTypes.contains(IDENTITY)) {
            for (Table table : tables) {
                if (!addTableScripts(table, context)) {
                    continue;
                }
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(context.getCreateScripts(column.getSequence()));
                    }
                }
            }
        }
        if (objectTypes.contains(TABLE)) {
            ScriptGeneratorManager tableScriptGeneratorManager = new ScriptGeneratorManager(context);
            Collection<Table> tableFromContext = (Collection<Table>)
                    tableScriptGeneratorManager.getAttributes().get(TABLES);
            tableScriptGeneratorManager.getObjectTypes().remove(FOREIGN_KEY);
            for (Table table : tables) {
                if (!addTableScripts(table, context)) {
                    continue;
                }
                scripts.addAll(tableScriptGeneratorManager.getCreateScripts(table));
                tableFromContext.add(table);
            }
        }
        if (objectTypes.contains(INDEX) && !dialect.supportsIndexInCreateTable()) {
            Collection<String> indexes = newLinkedHashSet();
            for (Table table : tables) {
                boolean primary = false;
                for (Index index : table.getIndexes()) {
                    if (!primary && index.isPrimary()) {
                        primary = true;
                        continue;
                    }
                    if (!addTableScripts(index.getTable(), context)) {
                        continue;
                    }
                    indexes.addAll(context.getCreateScripts(index));
                }
            }
            scripts.addAll(indexes);
        }
        boolean createTriggers = objectTypes.contains(TRIGGER);
        boolean createColumnTriggers = objectTypes.contains(COLUMN_TRIGGER);
        if (createTriggers || createColumnTriggers) {
            Collection<String> triggers = newLinkedHashSet();
            for (Table table : tables) {
                if (!addTableScripts(table, context)) {
                    continue;
                }
                for (Trigger trigger : table.getTriggers()) {
                    if (trigger.getObjectType() == TRIGGER && createTriggers) {
                        triggers.addAll(context.getCreateScripts(trigger));
                    } else if (trigger.getObjectType() == COLUMN_TRIGGER && createColumnTriggers) {
                        triggers.addAll(context.getCreateScripts(trigger));
                    }
                }
            }
            scripts.addAll(triggers);
        }
        if (objectTypes.contains(FOREIGN_KEY)) {
            Collection<Table> generatedTables = (Collection<Table>) context.getAttributes().get(TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) context.getAttributes().get(FOREIGN_KEYS);
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    Table primaryTable = foreignKey.getPrimaryTable();
                    if (!addTableScripts(primaryTable, context) ||
                            !addTableScripts(foreignKey.getForeignTable(), context)) {
                        continue;
                    }
                    if (generatedTables.contains(primaryTable)) {
                        scripts.addAll(context.getCreateScripts(foreignKey));
                    } else {
                        pendingForeignKeys.put(primaryTable, foreignKey);
                    }
                }
            }
        }
        addForeignKeysScripts(scripts, false, context);
    }

    protected void addForeignKeysScripts(Collection<String> scripts, boolean force, ScriptGeneratorManager context) {
        Collection<MetaDataType> objectTypes = context.getObjectTypes();
        if (objectTypes.contains(FOREIGN_KEY)) {
            Collection<Table> generatedTables = (Collection<Table>) context.getAttributes().get(TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) context.getAttributes().get(FOREIGN_KEYS);
            for (ForeignKey foreignKey : newArrayList(pendingForeignKeys.values())) {
                Table primaryTable = foreignKey.getPrimaryTable();
                if (!addTableScripts(primaryTable, context) ||
                        !addTableScripts(foreignKey.getForeignTable(), context)) {
                    continue;
                }
                if (generatedTables.contains(primaryTable) || force) {
                    scripts.addAll(context.getCreateScripts(foreignKey));
                    pendingForeignKeys.remove(primaryTable, foreignKey);
                }
            }
        }
    }

    protected void addDropScripts(Collection<Table> tables, Collection<String> scripts,
                                  ScriptGeneratorManager context) {
        Collection<MetaDataType> objectTypes = context.getObjectTypes();
        Dialect dialect = context.getTargetDialect();
        if (objectTypes.contains(FOREIGN_KEY) && dialect.supportsDropConstraints()) {
            for (Table table : tables) {
                if (!addTableScripts(table, context)) {
                    continue;
                }
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    scripts.addAll(context.getDropScripts(foreignKey));
                }
            }
        }
        boolean dropTriggers = objectTypes.contains(TRIGGER);
        boolean dropColumnTriggers = objectTypes.contains(COLUMN_TRIGGER);
        if (dropTriggers || dropColumnTriggers) {
            Collection<String> triggers = newLinkedHashSet();
            for (Table table : tables) {
                if (!addTableScripts(table, context)) {
                    continue;
                }
                for (Trigger trigger : table.getTriggers()) {
                    if (trigger.getObjectType() == TRIGGER && dropTriggers) {
                        triggers.addAll(context.getDropScripts(trigger));
                    } else if (trigger.getObjectType() == COLUMN_TRIGGER && dropColumnTriggers) {
                        triggers.addAll(context.getDropScripts(trigger));
                    }
                }
            }
            scripts.addAll(triggers);
        }
        if (objectTypes.contains(TABLE)) {
            for (Table table : tables) {
                if (!addTableScripts(table, context)) {
                    continue;
                }
                scripts.addAll(context.getDropScripts(table));
            }
        }
        if (objectTypes.contains(IDENTITY) && dialect.supportsSequence()) {
            for (Table table : tables) {
                if (!addTableScripts(table, context)) {
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

    protected boolean addTableScripts(Table table, ScriptGeneratorManager context) {
        Collection<String> tableTypes = (Collection<String>) context.getAttributes().get(TABLE_TYPES);
        return tableTypes != null ? tableTypes.contains(table.getType()) : Table.TABLE.equals(table.getType());
    }

    protected void initScriptGeneratorContext(ScriptGeneratorManager context) {
        Map<String, Object> attributes = context.getAttributes();
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

    protected void releaseScriptGeneratorContext(ScriptGeneratorManager context) {
        Map<String, Object> attributes = context.getAttributes();
        attributes.remove(TABLES);
        attributes.remove(FOREIGN_KEYS);
    }
}
