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
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Table;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.utils.Collections.addIgnoreNull;
import static java.lang.Math.min;
import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class ForeignKeyScriptGenerator extends ScriptGeneratorBase<ForeignKey>
        implements ConstraintScriptGenerator<ForeignKey> {

    public ForeignKeyScriptGenerator() {
        super(ForeignKey.class);
    }

    /**
     * Fix for DB-953 generates:
     * 
     * <pre>
     * ALTER TABLE "table1" ADD FOREIGN KEY ("column1") REFERENCES "table2" ("column2");
     * </pre>
     * 
     * instead of
     * 
     * <pre>
     * ALTER TABLE "table1" ADD CONSTRAINT "fk1" FOREIGN KEY ("column1") REFERENCES "table2" ("column2");
     * </pre>
     *
     * @param foreignKey
     * @param scriptGeneratorManager
     * @return
     */
    @Override
    public Collection<Script> getCreateScripts(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ALTER TABLE ");
        buffer.append(getForeignTable(foreignKey, scriptGeneratorManager));
        buffer.append(" ADD ");

        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        String fkName = foreignKey.getName(dialect);
        if (fkName != null) {
            // In NuoDB, the CONSTRAINT keyword is only necessary
            // if you want to name the table constraint
            buffer.append("CONSTRAINT ");
            buffer.append(fkName);
            buffer.append(" ");
        }

        buffer.append(getConstraintScript(foreignKey, scriptGeneratorManager));
        return singleton(new Script(buffer.toString(), foreignKey.getTable(), dialect.requiresTableLockForDDL()));
    }

    /**
     * Fix for DB-953 generates
     * 
     * <pre>
     * ALTER TABLE "table1" DROP FOREIGN KEY ("column1") REFERENCES "table2";
     * </pre>
     *
     * @param foreignKey
     * @param scriptGeneratorManager
     * @return
     */
    @Override
    public Collection<Script> getDropScripts(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ALTER TABLE ");
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        buffer.append(getForeignTable(foreignKey, scriptGeneratorManager));
        buffer.append(' ');
        buffer.append(dialect.getDropForeignKey());
        buffer.append(' ');
        buffer.append("(");
        for (Iterator<Column> iterator = foreignKey.getForeignColumns().iterator(); iterator.hasNext();) {
            Column column = iterator.next();
            buffer.append(scriptGeneratorManager.getName(column));
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        buffer.append(")");
        buffer.append(" REFERENCES ");
        buffer.append(getPrimaryScriptGeneratorManager(foreignKey, scriptGeneratorManager)
                .getQualifiedName(foreignKey.getPrimaryTable()));
        return singleton(new Script(buffer.toString(), foreignKey.getTable(), dialect.requiresTableLockForDDL()));
    }

    protected String getForeignTable(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        return scriptGeneratorManager.getName(foreignKey.getForeignTable());
    }

    @Override
    public String getConstraintScript(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        StringBuilder buffer = new StringBuilder();
        buffer.append("FOREIGN KEY (");
        for (Iterator<Column> iterator = foreignKey.getForeignColumns().iterator(); iterator.hasNext();) {
            Column column = iterator.next();
            buffer.append(scriptGeneratorManager.getName(column));
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        buffer.append(")");
        buffer.append(" REFERENCES ");
        buffer.append(getPrimaryTable(foreignKey, scriptGeneratorManager));
        buffer.append(" (");
        for (Iterator<Column> iterator = foreignKey.getPrimaryColumns().iterator(); iterator.hasNext();) {
            Column column = iterator.next();
            buffer.append(scriptGeneratorManager.getName(column));
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        buffer.append(")");
        String updateAction = dialect.getUpdateAction(foreignKey.getUpdateAction());
        if (updateAction != null) {
            buffer.append(" ON UPDATE ");
            buffer.append(updateAction);
        }
        String deleteAction = dialect.getDeleteAction(foreignKey.getDeleteAction());
        if (deleteAction != null) {
            buffer.append(" ON DELETE ");
            buffer.append(deleteAction);
        }
        return buffer.toString();
    }

    protected String getPrimaryTable(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        ScriptGeneratorManager primaryScriptGeneratorManager = getPrimaryScriptGeneratorManager(foreignKey,
                scriptGeneratorManager);
        return ObjectUtils.equals(scriptGeneratorManager, primaryScriptGeneratorManager)
                ? scriptGeneratorManager.getName(foreignKey.getPrimaryTable())
                : primaryScriptGeneratorManager.getQualifiedName(foreignKey.getPrimaryTable());
    }

    /**
     * Fixes cross database foreign keys generation
     * https://github.com/nuodb/migration-tools/issues/3
     *
     * @param foreignKey
     *            to create foreign script generator context
     * @param scriptGeneratorManager
     *            current script generator context used for the session
     * @return source context if primary and foreign schemas are equals or
     *         changes target schema & catalog name to primary table schema &
     *         catalog.
     */
    protected ScriptGeneratorManager getPrimaryScriptGeneratorManager(ForeignKey foreignKey,
            ScriptGeneratorManager scriptGeneratorManager) {
        ScriptGeneratorManager primaryScriptGeneratorManager = new ScriptGeneratorManager(scriptGeneratorManager);
        if (scriptGeneratorManager.getTargetCatalog() == null && scriptGeneratorManager.getTargetSchema() == null) {

            Table primaryTable = foreignKey.getPrimaryTable();
            List<String> qualifiers = newArrayList();
            addIgnoreNull(qualifiers, primaryTable.getCatalog().getName());
            addIgnoreNull(qualifiers, primaryTable.getSchema().getName());
            Dialect targetDialect = scriptGeneratorManager.getTargetDialect();
            int maximum = 0;
            if (targetDialect.supportsCatalogs()) {
                maximum++;
            }
            if (targetDialect.supportsSchemas()) {
                maximum++;
            }
            int actual = min(qualifiers.size(), maximum);
            qualifiers = qualifiers.subList(qualifiers.size() - actual, qualifiers.size());
            int index = 0;
            if (targetDialect.supportsCatalogs()) {
                primaryScriptGeneratorManager.setTargetCatalog(qualifiers.get(index++));
            }
            if (targetDialect.supportsSchemas()) {
                primaryScriptGeneratorManager.setTargetSchema(qualifiers.get(index));
            }
        }
        return primaryScriptGeneratorManager;
    }
}
