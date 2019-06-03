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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.ColumnTrigger;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Trigger;

import java.util.Collection;

import static com.nuodb.migrator.context.ContextUtils.createService;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.MYSQL;
import static java.sql.Types.*;
import static java.util.Arrays.asList;

/**
 * @author Sergey Bushik
 */
public class MySQLOnUpdateTriggerTranslator extends TriggerTranslatorBase {

    private Collection<Integer> TYPES = asList(TIME, DATE, TIMESTAMP);

    public MySQLOnUpdateTriggerTranslator() {
        super(MYSQL);
    }

    @Override
    protected boolean supportsScript(TriggerScript script, TranslationContext context) {
        Column column = getColumn(script.getTrigger());
        return column != null && TYPES.contains(column.getTypeCode())
                && context.translate(new ColumnScript(column)) != null;
    }

    protected Column getColumn(Trigger trigger) {
        return trigger instanceof ColumnTrigger ? ((ColumnTrigger) trigger).getColumn() : null;
    }

    @Override
    public Script translate(TriggerScript script, TranslationContext context) {
        Column column = getColumn(script.getTrigger());
        DatabaseInfo databaseInfo = context.getSession().getDatabaseInfo();
        Dialect dialect = createService(DialectResolver.class).resolve(databaseInfo);
        StringBuilder translation = new StringBuilder();
        translation.append("NEW.");
        translation.append(dialect.getIdentifier(column.getName(), column));
        translation.append(" = '");
        translation.append(context.translate(new ColumnScript(column)).getScript());
        translation.append("';");
        return new SimpleScript(translation.toString());
    }
}
