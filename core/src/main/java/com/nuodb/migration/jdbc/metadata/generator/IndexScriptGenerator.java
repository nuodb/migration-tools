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

import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.Column;
import com.nuodb.migration.jdbc.metadata.Index;

import java.util.Collection;
import java.util.Iterator;

import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class IndexScriptGenerator extends ScriptGeneratorBase<Index> implements ConstraintScriptGenerator<Index> {

    public IndexScriptGenerator() {
        super(Index.class);
    }

    @Override
    public Collection<String> getCreateScripts(Index index, ScriptGeneratorContext scriptGeneratorContext) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE");
        if (index.isUnique()) {
            buffer.append(" UNIQUE");
        }
        buffer.append(" INDEX ");
        buffer.append(scriptGeneratorContext.getName(index));
        buffer.append(" ON ");
        buffer.append(scriptGeneratorContext.getQualifiedName(index.getTable()));
        buffer.append(" (");
        for (Iterator<Column> iterator = index.getColumns().iterator(); iterator.hasNext(); ) {
            Column column = iterator.next();
            buffer.append(scriptGeneratorContext.getName(column));
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        buffer.append(')');
        return singleton(buffer.toString());
    }

    @Override
    public Collection<String> getDropScripts(Index index, ScriptGeneratorContext scriptGeneratorContext) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("DROP INDEX ");
        buffer.append(scriptGeneratorContext.getName(index));

        Dialect dialect = scriptGeneratorContext.getDialect();
        if (dialect.supportsDropIndexOnTable()) {
            buffer.append(" ON ");
            buffer.append(scriptGeneratorContext.getQualifiedName(index.getTable()));
            buffer.append(' ');
        }

        if (dialect.supportsDropIndexIfExists()) {
            buffer.append(' ');
            buffer.append("IF EXISTS");
        }
        return singleton(buffer.toString());
    }

    public String getConstraintScript(Index index, ScriptGeneratorContext scriptGeneratorContext) {
        Dialect dialect = scriptGeneratorContext.getDialect();
        StringBuilder buffer = new StringBuilder();
        if (index.isUnique()) {
            buffer.append("UNIQUE");
        } else {
            buffer.append("INDEX");
        }
        buffer.append(" (");
        boolean nullable = false;
        for (Iterator<Column> iterator = index.getColumns().iterator(); iterator.hasNext(); ) {
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
