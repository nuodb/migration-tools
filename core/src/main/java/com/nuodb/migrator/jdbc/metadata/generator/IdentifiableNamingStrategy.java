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
import com.nuodb.migrator.jdbc.metadata.Identifiable;
import com.nuodb.migrator.jdbc.metadata.IdentifiableBase;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;

/**
 * @author Sergey Bushik
 */
public class IdentifiableNamingStrategy<T extends Identifiable> extends MetaDataHandlerBase implements NamingStrategy<T> {

    public IdentifiableNamingStrategy() {
        super(Identifiable.class);
    }

    public IdentifiableNamingStrategy(Class<? extends T> typeClass) {
        super(typeClass);
    }

    @Override
    public String getName(T object, ScriptGeneratorContext scriptGeneratorContext, boolean normalize) {
        Dialect dialect = normalize ? scriptGeneratorContext.getDialect() : null;
        String name = getIdentifiableName(object, scriptGeneratorContext);
        return IdentifiableBase.getName(dialect, name, object);
    }

    @Override
    public String getQualifiedName(T object, ScriptGeneratorContext scriptGeneratorContext, boolean normalize) {
        Dialect dialect = normalize ? scriptGeneratorContext.getDialect() : null;
        String name = getIdentifiableName(object, scriptGeneratorContext);
        return IdentifiableBase.getQualifiedName(dialect, scriptGeneratorContext.getTargetCatalog(),
                scriptGeneratorContext.getTargetSchema(), name, object);
    }

    protected String getIdentifiableName(T object, ScriptGeneratorContext scriptGeneratorContext) {
        return object.getName();
    }
}