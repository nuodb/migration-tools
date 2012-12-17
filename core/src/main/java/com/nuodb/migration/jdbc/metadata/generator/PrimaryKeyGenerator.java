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

import com.nuodb.migration.jdbc.metadata.Column;
import com.nuodb.migration.jdbc.metadata.PrimaryKey;

import java.util.Collection;
import java.util.Iterator;

import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public class PrimaryKeyGenerator extends ScriptGeneratorBase<PrimaryKey> implements ConstraintGenerator<PrimaryKey> {

    public PrimaryKeyGenerator() {
        super(PrimaryKey.class);
    }

    @Override
    public String getConstraintSql(PrimaryKey primaryKey, ScriptGeneratorContext context) {
        StringBuilder buffer = new StringBuilder("PRIMARY KEY (");
        for (Iterator<Column> iterator = primaryKey.getColumns().iterator(); iterator.hasNext(); ) {
            Column column = iterator.next();
            buffer.append(context.getName(column));
            if (iterator.hasNext()) {
                buffer.append(", ");
            }
        }
        return buffer.append(')').toString();
    }

    @Override
    public Collection<String> getCreateScripts(PrimaryKey primaryKey, ScriptGeneratorContext scriptGeneratorContext) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ALTER TABLE ");
        buffer.append(scriptGeneratorContext.getName(primaryKey.getTable()));
        buffer.append(" ADD ");
        buffer.append(getConstraintSql(primaryKey, scriptGeneratorContext));
        return singleton(buffer.toString());
    }

    @Override
    public Collection<String> getDropScripts(PrimaryKey primaryKey, ScriptGeneratorContext scriptGeneratorContext) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ALTER TABLE ");
        buffer.append(scriptGeneratorContext.getName(primaryKey.getTable()));
        buffer.append(" DROP PRIMARY KEY");
        return singleton(buffer.toString());
    }
}
