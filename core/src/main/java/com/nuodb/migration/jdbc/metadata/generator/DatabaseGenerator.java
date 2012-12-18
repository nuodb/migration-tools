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
import com.google.common.collect.Sets;
import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.*;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migration.jdbc.metadata.MetaDataType.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DatabaseGenerator extends ScriptGeneratorBase<Database> {

    private static final String PROCESSED_TABLES = "PROCESSED_TABLES";

    private static final String PENDING_FOREIGN_KEYS = "PENDING_FOREIGN_KEYS";

    private GroupScriptsBy groupScriptsBy = GroupScriptsBy.TABLE;

    public DatabaseGenerator() {
        super(Database.class);
    }

    @Override
    public Collection<String> getCreateScripts(Database database, ScriptGeneratorContext scriptGeneratorContext) {
        initScriptGeneratorContext(scriptGeneratorContext);
        Collection<String> scripts = newArrayList();
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : database.listTables()) {
                    Collection<Table> tables = Collections.singleton(table);
                    addCreateScripts(scriptGeneratorContext, scripts, tables);
                }
                addPendingForeignKeysScripts(scriptGeneratorContext, scripts, true);
                break;
            case META_DATA:
                Collection<Table> tables = database.listTables();
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
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : database.listTables()) {
                    Collection<Table> tables = Collections.singleton(table);
                    addDropScripts(scriptGeneratorContext, scripts, tables);
                }
                break;
            case META_DATA:
                Collection<Table> tables = database.listTables();
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
        switch (groupScriptsBy) {
            case TABLE:
                for (Table table : database.listTables()) {
                    Collection<Table> tables = Collections.singleton(table);
                    addDropScripts(scriptGeneratorContext, scripts, tables);
                    addCreateScripts(scriptGeneratorContext, scripts, tables);
                }
                addPendingForeignKeysScripts(scriptGeneratorContext, scripts, true);
                break;
            case META_DATA:
                Collection<Table> tables = database.listTables();
                addDropScripts(scriptGeneratorContext, scripts, tables);
                addCreateScripts(scriptGeneratorContext, scripts, tables);
                break;
        }
        releaseScriptGeneratorContext(scriptGeneratorContext);
        return scripts;
    }

    protected void addCreateScripts(ScriptGeneratorContext scriptGeneratorContext,
                                    Collection<String> scripts, Collection<Table> tables) {
        Collection<MetaDataType> metaDataTypes = scriptGeneratorContext.getMetaDataTypes();
        Dialect dialect = scriptGeneratorContext.getDialect();
        if (metaDataTypes.contains(SEQUENCE)) {
            for (Table table : tables) {
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(scriptGeneratorContext.getCreateScripts(column.getSequence()));
                    }
                }
            }
        }
        if (metaDataTypes.contains(TABLE)) {
            ScriptGeneratorContext tableScriptGeneratorContext =
                    new SimpleScriptGeneratorContext(scriptGeneratorContext);
            Collection<Table> processedTables = (Collection<Table>)
                    tableScriptGeneratorContext.get(PROCESSED_TABLES);
            tableScriptGeneratorContext.getMetaDataTypes().remove(FOREIGN_KEY);
            for (Table table : tables) {
                processedTables.add(table);
                if (Table.TABLE.equals(table.getType())) {
                    scripts.addAll(tableScriptGeneratorContext.getCreateScripts(table));
                }
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
                    scripts.addAll(scriptGeneratorContext.getCreateScripts(index));
                }
            }
        }
        if (metaDataTypes.contains(FOREIGN_KEY)) {
            Collection<Table> processedTables =
                    (Collection<Table>) scriptGeneratorContext.get(PROCESSED_TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) scriptGeneratorContext.get(PENDING_FOREIGN_KEYS);
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    Table targetTable = foreignKey.getTargetTable();
                    if (processedTables.contains(targetTable)) {
                        scripts.addAll(scriptGeneratorContext.getCreateScripts(foreignKey));
                    } else {
                        pendingForeignKeys.put(targetTable, foreignKey);
                    }
                }
            }
        }
        addPendingForeignKeysScripts(scriptGeneratorContext, scripts, false);
    }

    protected void addPendingForeignKeysScripts(ScriptGeneratorContext scriptGeneratorContext,
                                                Collection<String> scripts, boolean forceAdd) {
        Collection<MetaDataType> metaDataTypes = scriptGeneratorContext.getMetaDataTypes();
        if (metaDataTypes.contains(FOREIGN_KEY)) {
            Collection<Table> processedTables =
                    (Collection<Table>) scriptGeneratorContext.get(PROCESSED_TABLES);
            Multimap<Table, ForeignKey> pendingForeignKeys =
                    (Multimap<Table, ForeignKey>) scriptGeneratorContext.get(PENDING_FOREIGN_KEYS);
            for (ForeignKey foreignKey : newArrayList(pendingForeignKeys.values())) {
                Table targetTable = foreignKey.getTargetTable();
                if (processedTables.contains(targetTable) || forceAdd) {
                    scripts.addAll(scriptGeneratorContext.getCreateScripts(foreignKey));
                    pendingForeignKeys.remove(targetTable, foreignKey);
                }
            }
        }
    }

    protected void addDropScripts(ScriptGeneratorContext scriptGeneratorContext,
                                  Collection<String> scripts, Collection<Table> tables) {
        Collection<MetaDataType> metaDataTypes = scriptGeneratorContext.getMetaDataTypes();
        Dialect dialect = scriptGeneratorContext.getDialect();
        if (metaDataTypes.contains(FOREIGN_KEY) && dialect.supportsDropConstraints()) {
            for (Table table : tables) {
                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    scripts.addAll(scriptGeneratorContext.getDropScripts(foreignKey));
                }
            }
        }
        if (metaDataTypes.contains(TABLE)) {
            for (Table table : tables) {
                if (Table.TABLE.equals(table.getType())) {
                    scripts.addAll(scriptGeneratorContext.getDropScripts(table));
                }
            }
        }
        if (metaDataTypes.contains(SEQUENCE) && dialect.supportsSequence()) {
            for (Table table : tables) {
                for (Column column : table.getColumns()) {
                    if (column.getSequence() != null) {
                        scripts.addAll(scriptGeneratorContext.getDropScripts(column.getSequence()));
                    }
                }
            }
        }
    }

    protected void initScriptGeneratorContext(ScriptGeneratorContext scriptGeneratorContext) {
        scriptGeneratorContext.put(PROCESSED_TABLES, Sets.newLinkedHashSet());
        scriptGeneratorContext.put(PENDING_FOREIGN_KEYS,
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
        scriptGeneratorContext.remove(PROCESSED_TABLES);
        scriptGeneratorContext.remove(PENDING_FOREIGN_KEYS);
    }

    public GroupScriptsBy getGroupScriptsBy() {
        return groupScriptsBy;
    }

    public void setGroupScriptsBy(GroupScriptsBy groupScriptsBy) {
        this.groupScriptsBy = groupScriptsBy;
    }
}
