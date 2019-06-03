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

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;

import java.sql.Types;
import java.util.Collection;

import static com.google.common.collect.Sets.newTreeSet;
import static java.lang.String.CASE_INSENSITIVE_ORDER;

/**
 * @author Sergey Bushik
 */
public class CurrentTimestampTranslator extends ColumnTranslatorBase<ColumnScript> {

    private Collection<String> aliases = newTreeSet(CASE_INSENSITIVE_ORDER);
    private String timestamp;
    private boolean literal;

    public CurrentTimestampTranslator(DatabaseInfo databaseInfo, Collection<String> aliases, String timestamp) {
        this(databaseInfo, aliases, timestamp, false);
    }

    public CurrentTimestampTranslator(DatabaseInfo databaseInfo, Collection<String> aliases, String timestamp,
            boolean literal) {
        super(databaseInfo);
        this.aliases.addAll(aliases);
        this.timestamp = timestamp;
        this.literal = literal;
    }

    @Override
    protected boolean supportsScript(ColumnScript script, TranslationContext context) {
        boolean supports;
        switch (script.getColumn().getTypeCode()) {
        case Types.TIME:
        case Types.DATE:
        case Types.TIMESTAMP:
            supports = true;
            break;
        default:
            supports = false;
            break;
        }
        return supports && script.getScript() != null && aliases.contains(script.getScript());
    }

    @Override
    public Script translate(ColumnScript script, TranslationContext context) {
        return new SimpleScript(timestamp, literal);
    }
}
