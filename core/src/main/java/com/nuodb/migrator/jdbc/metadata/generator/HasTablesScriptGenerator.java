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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.HasTables;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.filter.HasTablesFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.utils.SequenceUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager.FOREIGN_KEYS;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager.TABLES;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager.UNIQUE_CONSTRAINTS;
import static com.nuodb.migrator.utils.SequenceUtils.getStandaloneSequences;
import static java.util.Collections.singleton;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class HasTablesScriptGenerator<H extends HasTables> extends MetaDataHandlerBase implements ScriptGenerator<H> {

    public static final String GROUP_SCRIPTS_BY = "group.scripts.by";

    public HasTablesScriptGenerator() {
        this((Class<H>) HasTables.class);
    }

    protected HasTablesScriptGenerator(Class<H> objectClass) {
        super(objectClass);
    }

    protected HasTables getTables(HasTables tables, ScriptGeneratorManager scriptGeneratorManager) {
        return new HasTablesFilter(tables, scriptGeneratorManager.getMetaDataFilterManager());
    }

    @Override
    public Collection<Script> getScripts(HasTables tables, ScriptGeneratorManager scriptGeneratorManager) {
        initScriptGeneratorContext(scriptGeneratorManager);
        try {
            Collection<Script> scripts = newArrayList();
            boolean addSequences = scriptGeneratorManager.getObjectTypes().contains(SEQUENCE);
            if (addSequences) {
                MetaDataFilterManager filterManager = scriptGeneratorManager.getMetaDataFilterManager();
                MetaDataFilter sequenceFilter = filterManager.getMetaDataFilter(SEQUENCE);
                for (Sequence sequence : getStandaloneSequences(tables)) {
                    if (sequenceFilter == null || sequenceFilter.accepts(sequence)) {
                        scripts.addAll(scriptGeneratorManager.getScripts(sequence));
                    }
                }
            }
            GroupScriptsBy groupScriptsBy = getGroupScriptsBy(scriptGeneratorManager);
            switch (groupScriptsBy) {
            case TABLE:
                for (Table table : getTables(tables, scriptGeneratorManager).getTables()) {
                    scripts.addAll(scriptGeneratorManager.getScripts(table));
                }
                migratorSummary(scriptGeneratorManager);
                addCreateForeignKeysScripts(scripts, true, scriptGeneratorManager);
                break;
            case META_DATA:
                Collection<MetaDataType> objectTypes = scriptGeneratorManager.getObjectTypes();
                try {
                    for (MetaDataType objectType : newArrayList(SEQUENCE, TABLE, PRIMARY_KEY, INDEX, TRIGGER,
                            COLUMN_TRIGGER, FOREIGN_KEY)) {
                        if (objectTypes.contains(objectType)) {
                            scriptGeneratorManager.setObjectTypes(singleton(objectType));
                            for (Table table : getTables(tables, scriptGeneratorManager).getTables()) {
                                scripts.addAll(scriptGeneratorManager.getScripts(table));
                            }
                        }
                    }
                } finally {
                    scriptGeneratorManager.setObjectTypes(objectTypes);
                }
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

    protected void addCreateForeignKeysScripts(Collection<Script> scripts, boolean force,
            ScriptGeneratorManager scriptGeneratorManager) {
        boolean createForeignKeys = scriptGeneratorManager.getObjectTypes().contains(FOREIGN_KEY);
        if (!createForeignKeys) {
            return;
        }
        Collection<Table> tables = (Collection<Table>) scriptGeneratorManager.getAttribute(TABLES);
        Multimap<Table, ForeignKey> foreignKeys = (Multimap<Table, ForeignKey>) scriptGeneratorManager
                .getAttribute(FOREIGN_KEYS);
        for (ForeignKey foreignKey : newArrayList(foreignKeys.values())) {
            Table primaryTable = foreignKey.getPrimaryTable();
            if (tables.contains(primaryTable) || force) {
                scripts.addAll(scriptGeneratorManager.getCreateScripts(foreignKey));
                foreignKeys.remove(primaryTable, foreignKey);
            }
        }
    }

    public void migratorSummary(ScriptGeneratorManager scriptGeneratorManager) {
        String catalog = scriptGeneratorManager.getSourceCatalog();
        String schema = scriptGeneratorManager.getSourceSchema();
        if (!tableNames.isEmpty() && !datatypes.isEmpty()) {
            String tables[] = tableNames.toArray(new String[tableNames.size()]);
            Arrays.sort(tables);
            String table = org.apache.commons.lang3.StringUtils.EMPTY;
            int count = 0;
            if (!(catalog == null) || !(schema == null)) {
                catalog = (catalog == null ? "<" + catalog + ">" : catalog);
                schema = (schema == null ? "<" + schema + ">" : schema);
                logger.info("\n\n");
                logger.info("*****************************");
                logger.info("***** Migration Summary *****");
                logger.info("*****************************");
                logger.info(format(" Catalog = %s", catalog));
                logger.info(format(" Schema = %s\n", schema));
                logger.info("*****************************");
                logger.info(format("******** Tables *************"));
                logger.info("*****************************");
                for (int i = 0; i < tables.length; i++) {
                    table = table + tables[i] + "  |  ";
                    count++;
                    if ((count % 3 == 0) || (count % 2 == 0 && tables.length == count)
                            || (count % 1 == 0 && tables.length == count)) {
                        logger.info(format(" %s", table));
                        table = org.apache.commons.lang3.StringUtils.EMPTY;
                    }
                }
            }
            if (!(datatypes.isEmpty())) {
                logger.info("\n");
                logger.info("***********************************");
                logger.info("**** Datatypes Mapping Summary ****");
                logger.info("***********************************");
                logger.info(format(" Source" + " Datatypes \t" + " NuoDB Datatypes"));
                logger.info(" ---------------- \t ----------------");
                for (Entry<String, String> entry : datatypes.entrySet()) {
                    logger.info(format(" " + entry.getKey() + "\t\t " + entry.getValue()));
                }
                logger.info("\n\n");
            }
        }
    }

    protected void initScriptGeneratorContext(ScriptGeneratorManager scriptGeneratorManager) {
        scriptGeneratorManager.addAttribute(TABLES, newLinkedHashSet());
        scriptGeneratorManager.addAttribute(FOREIGN_KEYS, HashMultimap.create());
        scriptGeneratorManager.addAttribute(UNIQUE_CONSTRAINTS, true);
    }

    protected void releaseScriptGeneratorContext(ScriptGeneratorManager scriptGeneratorManager) {
        scriptGeneratorManager.removeAttribute(TABLES);
        scriptGeneratorManager.removeAttribute(FOREIGN_KEYS);
        scriptGeneratorManager.removeAttribute(UNIQUE_CONSTRAINTS);
    }
}
