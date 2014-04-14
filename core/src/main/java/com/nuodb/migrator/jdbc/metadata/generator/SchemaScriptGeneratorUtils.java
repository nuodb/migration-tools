/**
 * Copyright (c) 2014, NuoDB, Inc.
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
import com.nuodb.migrator.jdbc.metadata.Schema;

/**
 * @author Sergey Bushik
 */
public class SchemaScriptGeneratorUtils {

    public static String getUseSchema(ScriptGeneratorManager scriptGeneratorManager) {
        return getUseSchema(scriptGeneratorManager, null);
    }

    public static String getUseSchema(ScriptGeneratorManager scriptGeneratorManager, Schema schema) {
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        String useSchema = null;
        if (scriptGeneratorManager.getTargetSchema() != null) {
            useSchema = dialect.getUseSchema(scriptGeneratorManager.getTargetSchema(), true);
        } else if (scriptGeneratorManager.getTargetCatalog() != null) {
            useSchema = dialect.getUseCatalog(scriptGeneratorManager.getTargetCatalog(), true);
        }
        if (useSchema == null) {
            useSchema = schema.getIdentifier() != null ?
                    dialect.getUseSchema(scriptGeneratorManager.getName(schema)) :
                    dialect.getUseCatalog(scriptGeneratorManager.getName(schema.getCatalog()));
        }
        return useSchema;
    }
}
