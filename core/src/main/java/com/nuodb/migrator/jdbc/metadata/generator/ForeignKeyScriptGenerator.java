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

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Schema;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.Iterator;

import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class ForeignKeyScriptGenerator extends ScriptGeneratorBase<ForeignKey> implements ConstraintScriptGenerator<ForeignKey> {

    public ForeignKeyScriptGenerator() {
        super(ForeignKey.class);
    }

    /**
     * Fix for DB-953 generates:
     * <pre>ALTER TABLE "table1" ADD FOREIGN KEY ("column1") REFERENCES "table2" ("column2");</pre>
     * instead of
     * <pre>ALTER TABLE "table1" ADD CONSTRAINT "fk1" FOREIGN KEY ("column1") REFERENCES "table2" ("column2");</pre>
     *
     * @param foreignKey
     * @param scriptGeneratorManager
     * @return
     */
    @Override
    public Collection<String> getCreateScripts(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ALTER TABLE ");
        buffer.append(scriptGeneratorManager.getQualifiedName(foreignKey.getForeignTable()));
        buffer.append(" ADD ");
        buffer.append(getConstraintScript(foreignKey, scriptGeneratorManager));
        return singleton(buffer.toString());
    }

    /**
     * Fix for DB-953 generates
     * <pre>ALTER TABLE "table1" DROP FOREIGN KEY ("column1") REFERENCES "table2";</pre>
     *
     * @param foreignKey
     * @param scriptGeneratorManager
     * @return
     */
    @Override
    public Collection<String> getDropScripts(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ALTER TABLE ");
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        buffer.append(scriptGeneratorManager.getQualifiedName(foreignKey.getForeignTable()));
        buffer.append(' ');
        buffer.append(dialect.getDropForeignKey());
        buffer.append(' ');
        for (Iterator<Column> iterator = foreignKey.getForeignColumns().iterator(); iterator.hasNext(); ) {
            Column column = iterator.next();
            buffer.append(scriptGeneratorManager.getName(column));
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        buffer.append(")");
        buffer.append(" REFERENCES ");
        buffer.append(getForeignScriptGeneratorManager(foreignKey, scriptGeneratorManager).getQualifiedName(
                foreignKey.getPrimaryTable()));
        return singleton(buffer.toString());
    }

    @Override
    public String getConstraintScript(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        StringBuilder buffer = new StringBuilder();
        buffer.append("FOREIGN KEY (");
        for (Iterator<Column> iterator = foreignKey.getForeignColumns().iterator(); iterator.hasNext(); ) {
            Column column = iterator.next();
            buffer.append(scriptGeneratorManager.getName(column));
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        buffer.append(")");
        buffer.append(" REFERENCES ");
        buffer.append(getForeignScriptGeneratorManager(foreignKey, scriptGeneratorManager).getQualifiedName(
                foreignKey.getPrimaryTable()));
        buffer.append(" (");
        for (Iterator<Column> iterator = foreignKey.getPrimaryColumns().iterator(); iterator.hasNext(); ) {
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

    /**
     * Fixes cross database foreign keys generation https://github.com/nuodb/migrator-tools/issues/3
     *
     * @param foreignKey             to create foreign script generator context
     * @param scriptGeneratorManager current script generator context used for the session
     * @return source context if primary and foreign schemas are equals or changes target schema & catalog name to
     *         foreign table schema & catalog.
     */
    protected ScriptGeneratorManager getForeignScriptGeneratorManager(ForeignKey foreignKey,
                                                                      ScriptGeneratorManager scriptGeneratorManager) {
        Schema primarySchema = foreignKey.getPrimaryTable().getSchema();
        Schema foreignSchema = foreignKey.getForeignTable().getSchema();
        if (ObjectUtils.equals(primarySchema, foreignSchema)) {
            return scriptGeneratorManager;
        } else {
            ScriptGeneratorManager foreignScriptGeneratorManager = new ScriptGeneratorManager(scriptGeneratorManager);
            foreignScriptGeneratorManager.setTargetCatalog(foreignSchema.getCatalog().getName());
            foreignScriptGeneratorManager.setTargetSchema(foreignSchema.getName());
            return foreignScriptGeneratorManager;
        }
    }
}
