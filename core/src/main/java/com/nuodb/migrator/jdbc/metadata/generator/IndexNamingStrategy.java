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

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Index;

import static com.nuodb.migrator.utils.StringUtils.*;
import static com.nuodb.migrator.utils.StringUtils.upperCase;

/**
 * @author Sergey Bushik
 */
public class IndexNamingStrategy extends IdentifiableNamingStrategy<Index> {

    private static final String PREFIX = "IDX";
    private static final String UNIQUE = "UNIQUE";
    private static final char DELIMITER = '_';

    public IndexNamingStrategy() {
        super(Index.class);
    }

    @Override
    protected String getIdentifiableName(Index index, ScriptGeneratorManager scriptGeneratorManager) {
        StringBuilder prefix = new StringBuilder();
        prefix.append(PREFIX);
        if (index.isUnique()) {
            prefix.append("_");
            prefix.append(UNIQUE);
        }
        StringBuilder qualifier = new StringBuilder();
        qualifier.append(scriptGeneratorManager.getName(index.getTable(), false));
        for (Column column : index.getColumns()) {
            qualifier.append("_");
            qualifier.append(scriptGeneratorManager.getName(column, false));
        }
        StringBuilder buffer = new StringBuilder();
        if (isLowerCase(qualifier)) {
            buffer.append(lowerCase(prefix));
        } else if (isCapitalizedCase(qualifier, DELIMITER)) {
            buffer.append(capitalizedCase(prefix, '_'));
        } else {
            buffer.append(upperCase(prefix));
        }
        buffer.append('_');
        buffer.append(qualifier);
        return buffer.toString();
    }
}