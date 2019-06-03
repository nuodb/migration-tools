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
import com.nuodb.migrator.jdbc.metadata.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class IndexScriptGenerator extends ScriptGeneratorBase<Index> implements ConstraintScriptGenerator<Index> {

    private final transient Logger logger = LoggerFactory.getLogger(getClass());

    public IndexScriptGenerator() {
        super(Index.class);
    }

    @Override
    public Collection<Script> getCreateScripts(Index index, ScriptGeneratorManager scriptGeneratorManager) {
        /**
         * Drop all indexes other than BTREE
         */
        if ((index.getType() != null) && (!index.isBtree())) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Only BTREE index is supported, %s is %s index", index, index.getType()));
            }
            return emptySet();
        }

        if (index.getExpression() != null) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Expression based indexes not supported %s", index));
            }
            return emptySet();
        } else {
            StringBuilder buffer = new StringBuilder();
            buffer.append("CREATE");
            if (index.isUnique()) {
                buffer.append(" UNIQUE");
            }
            buffer.append(" INDEX ");
            buffer.append(scriptGeneratorManager.getName(index));
            buffer.append(" ON ");
            buffer.append(scriptGeneratorManager.getQualifiedName(index.getTable()));
            buffer.append(" (");
            for (Iterator<Column> iterator = index.getColumns().iterator(); iterator.hasNext();) {
                Column column = iterator.next();
                buffer.append(scriptGeneratorManager.getName(column));
                if (iterator.hasNext()) {
                    buffer.append(", ");
                }
            }
            buffer.append(')');
            Dialect dialect = scriptGeneratorManager.getTargetDialect();
            return singleton(new Script(buffer.toString(), index.getTable(), dialect.requiresTableLockForDDL()));
        }
    }

    @Override
    public Collection<Script> getDropScripts(Index index, ScriptGeneratorManager scriptGeneratorManager) {
        /**
         * Drop all indexes other than BTREE
         */
        if ((index.getType() != null) && (!index.isBtree())) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Only BTREE index is supported, %s is %s index", index, index.getType()));
            }
            return emptySet();
        }

        if (index.getExpression() != null) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Index expressions are not supported %s", index));
            }
            return emptySet();
        } else {
            StringBuilder buffer = new StringBuilder();
            buffer.append("DROP INDEX ");
            buffer.append(scriptGeneratorManager.getName(index));

            Dialect dialect = scriptGeneratorManager.getTargetDialect();
            if (dialect.supportsDropIndexOnTable()) {
                buffer.append(' ');
                buffer.append("ON");
                buffer.append(' ');
                buffer.append(scriptGeneratorManager.getQualifiedName(index.getTable()));
            }
            if (dialect.supportsDropIndexIfExists()) {
                buffer.append(' ');
                buffer.append("IF EXISTS");
            }
            return singleton(new Script(buffer.toString()));
        }
    }

    public String getConstraintScript(Index index, ScriptGeneratorManager scriptGeneratorManager) {
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        StringBuilder buffer = new StringBuilder();
        if (index.isUnique()) {
            buffer.append("UNIQUE ");
        }
        if (!index.isUniqueConstraint()) {
            // CONSATRINT + $NAME + UNIQUE -> UNIQUE CONSTRAINT (type 4)
            // CONSTRAINT + $NAME + UNIQUE KEY $NAME -> UNIQUE INDEX (type 1)
            buffer.append("KEY ");
            buffer.append(scriptGeneratorManager.getName(index));
        }
        buffer.append("(");
        boolean nullable = false;
        for (Iterator<Column> iterator = index.getColumns().iterator(); iterator.hasNext();) {
            Column column = iterator.next();
            if (column.isNullable()) {
                nullable = true;
            }
            buffer.append(column.getName(dialect));
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        buffer.append(')');
        return !nullable || dialect.supportsNotNullUnique() ? buffer.toString() : null;
    }
}
