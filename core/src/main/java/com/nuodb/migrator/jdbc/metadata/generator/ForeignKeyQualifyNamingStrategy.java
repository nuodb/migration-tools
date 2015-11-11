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

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;

import static com.nuodb.migrator.utils.StringUtils.*;
import static com.nuodb.migrator.utils.StringUtils.isUpperCase;
import static com.nuodb.migrator.utils.StringUtils.upperCase;

/**
 * Produces qualified foreign key name.
 *
 * @author Sergey Bushik
 */
public class ForeignKeyQualifyNamingStrategy extends IdentifiableNamingStrategy<ForeignKey> {

    private static final String PREFIX = "FK";

    public ForeignKeyQualifyNamingStrategy() {
        super(ForeignKey.class, PREFIX);
    }

    @Override
    protected String getNonPrefixedName(ForeignKey foreignKey, ScriptGeneratorManager scriptGeneratorManager) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(scriptGeneratorManager.getQualifiedName(foreignKey.getPrimaryTable(), false));
        for (Column column : foreignKey.getPrimaryColumns()) {
            buffer.append(getDelimiter());
            buffer.append(scriptGeneratorManager.getName(column, false));
        }
        buffer.append(getDelimiter());
        buffer.append(scriptGeneratorManager.getQualifiedName(foreignKey.getForeignTable(), false));
        for (Column column : foreignKey.getForeignColumns()) {
            buffer.append(getDelimiter());
            buffer.append(scriptGeneratorManager.getName(column, false));
        }
        String tableName = scriptGeneratorManager.getName(foreignKey.getTable(), false);
        String nonPrefixedName;
        if (isLowerCase(tableName)) {
            nonPrefixedName = lowerCase(buffer);
        } else if (isCapitalizedCase(tableName)) {
            nonPrefixedName = capitalizedCase(buffer);
        } else if (isUpperCase(tableName)) {
            nonPrefixedName = upperCase(buffer);
        } else {
            nonPrefixedName = buffer.toString();
        }
        return nonPrefixedName;
    }
}
