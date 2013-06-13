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

    private static final String TABLES_GENERATED = "TABLES_GENERATED";

    private static final String FOREIGN_KEY_PENDING_PRIMARY_TABLE = "FOREIGN_KEY_PENDING_PRIMARY_TABLE";

    public HasTablesScriptGenerator() {
        this((Class<H>) HasTables.class);
    }

    protected HasTablesScriptGenerator(Class<H> objectClass) {
        super(objectClass);
    }

    @Override
    public Collection<String> getCreateScripts(H tables, ScriptGeneratorContext context) {
        return getHasTablesCreateScripts(getTables(context, tables), context);
    }

    @Override
    public Collection<String> getDropScripts(H tables, ScriptGeneratorContext context) {
        return getHasTablesDropScripts(getTables(context, tables), context);
    }

    @Override
    public Collection<String> getDropCreateScripts(H tables, ScriptGeneratorContext context) {
        return getHasTablesDropCreateScripts(getTables(context, tables), context);
    }

    protected HasTables getTables(ScriptGeneratorContext scriptGeneratorContext, H tables) {
        return tables;
    }

    protected Collection<String> getHasTablesCreateScripts(HasTables tables, ScriptGeneratorContext context) {
        initScriptGeneratorContext(context);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(context);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addCreateScripts(context, scripts, singleton(table));
                    }
                    addForeignKeysScripts(context, scripts, true);
                    break;
                case META_DATA:
                    addCreateScripts(context, scripts, tables.getTables());
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(context);
        }
    }

    protected Collection<String> getHasTablesDropScripts(HasTables tables, ScriptGeneratorContext context) {
        initScriptGeneratorContext(context);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(context);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addDropScripts(context, scripts, singleton(table));
                    }
                    break;
                case META_DATA:
                    addCreateScripts(context, scripts, tables.getTables());
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(context);
        }
    }

    protected Collection<String> getHasTablesDropCreateScripts(HasTables tables, ScriptGeneratorContext context) {
        initScriptGeneratorContext(context);
        try {
            Collection<String> scripts = newArrayList();
            GroupScriptsBy groupScriptsBy = (GroupScriptsBy) context.getAttributes().get(
                    GROUP_SCRIPTS_BY);
            switch (groupScriptsBy) {
                case TABLE:
                    for (Table table : tables.getTables()) {
                        addDropScripts(context, scripts, singleton(table));
                        addCreateScripts(context, scripts, singleton(table));
                    }
                    addForeignKeysScripts(context, scripts, true);
                    break;
                case META_DATA:
                    addDropScripts(context, scripts, tables.getTables());
                    addCreateScripts(context, scripts, tables.getTables());
                    break;
            }
            return scripts;
        } finally {
            releaseScriptGeneratorContext(context);
        }
    }

    protected GroupScriptsBy getGroupScriptsBy(ScriptGeneratorContext context) {
        GroupScriptsBy groupScriptsBy = (GroupScriptsBy) context.getAttributes().get(GROUP_SCRIPTS_BY);
        return groupScriptsBy != null ? GroupScriptsBy.TABLE : null;
    }

    protected void addCreateScripts(ScriptGeneratorContext context, Collection<String> scripts,
                                    Collection<Table> tables) {
        Collection<MetaDataType> objectTypes = context.getObjectTypes();
        Dialect dialect = context.getDialect();
        if (objectTypes.contains(AUTO_INCREMENT)) {
            for (Table table : tables) {
                if (!isGenerateScript(context, table)) {
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
            ScriptGeneratorContext tableScriptGeneratorContext = new ScriptGeneratorContext(context);
            Collection<Table> tablesGenerated = (Collection<Table>)
                    tableScriptGeneratorContext.getAttributes().get(TABLES_GENERATED);
            tableScriptGeneratorContext.getObjectTypes().remove(FOREIGN_KEY);
            for (Table table : tables) {
                if (!isGenerateScript(context, table)) {
                    continue;
                }
                scripts.addAll(tableScriptGeneratorContext.getCreateScripts(table));
                tablesGenerated.add(table);
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
                    if (!isGenerateScript(context, index.getTable())) {
                        continue;
                    }
                    scripts.addAll(context.getCreateScripts(index));
                }
            }
        }
        if (objectTypes.contains(FOREIGN_KEY)) {
            Collection<Table> tablesGenerated = (Collection<Table>) context.getAttributes().get(TABLES_GENERATED);
            Multimap<Table, ForeignKey> foreignKeyPending =
                    (Multimap<Table, ForeignKey>) context.getAttributes().get(FOREIGN_KEY_PENDING_PRIMARY_TABLE);
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    Table primaryTable = foreignKey.getPrimaryTable();
                    if (!isGenerateScript(context, primaryTable) ||
                            !isGenerateScript(context, foreignKey.getForeignTable())) {
                        continue;
                    }
                    if (tablesGenerated.contains(primaryTable)) {
                        scripts.addAll(context.getCreateScripts(foreignKey));
                    } else {
                        foreignKeyPending.put(primaryTable, foreignKey);
                    }
                }
            }
        }
        addForeignKeysScripts(context, scripts, false);
    }

    protected void addForeignKeysScripts(ScriptGeneratorContext context, Collection<String> scripts, boolean force) {
        Collection<MetaDataType> objectTypes = context.getObjectTypes();
        if (objectTypes.contains(FOREIGN_KEY)) {
            Collection<Table> tablesGenerated = (Collection<Table>) context.getAttributes().get(TABLES_GENERATED);
            Multimap<Table, ForeignKey> foreignKeyPending =
                    (Multimap<Table, ForeignKey>) context.getAttributes().get(FOREIGN_KEY_PENDING_PRIMARY_TABLE);
            for (ForeignKey foreignKey : newArrayList(foreignKeyPending.values())) {
                Table primaryTable = foreignKey.getPrimaryTable();
                if (!isGenerateScript(context, primaryTable) ||
                        !isGenerateScript(context, foreignKey.getForeignTable())) {
                    continue;
                }
                if (tablesGenerated.contains(primaryTable) || force) {
                    scripts.addAll(context.getCreateScripts(foreignKey));
                    foreignKeyPending.remove(primaryTable, foreignKey);
                }
            }
        }
    }

    protected void addDropScripts(ScriptGeneratorContext context, Collection<String> scripts, Collection<Table> tables) {
        Collection<MetaDataType> objectTypes = context.getObjectTypes();
        Dialect dialect = context.getDialect();
        if (objectTypes.contains(FOREIGN_KEY) && dialect.supportsDropConstraints()) {
            for (Table table : tables) {
                if (!isGenerateScript(context, table)) {
                    continue;
                }
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    scripts.addAll(context.getDropScripts(foreignKey));
                }
            }
        }
        if (objectTypes.contains(TABLE)) {
            for (Table table : tables) {
                if (!isGenerateScript(context, table)) {
                    continue;
                }
                scripts.addAll(context.getDropScripts(table));
            }
        }
        if (objectTypes.contains(AUTO_INCREMENT) && dialect.supportsSequence()) {
            for (Table table : tables) {
                if (!isGenerateScript(context, table)) {
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

    protected boolean isGenerateScript(ScriptGeneratorContext context, Table table) {
        Collection<String> tableTypes = (Collection<String>) context.getAttributes().get(TABLE_TYPES);
        return tableTypes != null ? tableTypes.contains(table.getType()) : Table.TABLE.equals(table.getType());
    }

    protected void initScriptGeneratorContext(ScriptGeneratorContext context) {
        Map<String, Object> attributes = context.getAttributes();
        attributes.put(TABLES_GENERATED, newLinkedHashSet());
        attributes.put(FOREIGN_KEY_PENDING_PRIMARY_TABLE,
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
        attributes.remove(TABLES_GENERATED);
        attributes.remove(FOREIGN_KEY_PENDING_PRIMARY_TABLE);
    }
}
