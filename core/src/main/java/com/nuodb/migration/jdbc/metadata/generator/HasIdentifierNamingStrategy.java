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
import com.nuodb.migration.jdbc.metadata.HasIdentifier;
import com.nuodb.migration.jdbc.metadata.HasIdentifierBase;

import static org.apache.commons.codec.binary.Hex.encodeHex;
import static org.apache.commons.codec.digest.DigestUtils.md5;

/**
 * @author Sergey Bushik
 */
public class HasIdentifierNamingStrategy<R extends HasIdentifier> extends GeneratorServiceBase<R> implements NamingStrategy<R> {

    public HasIdentifierNamingStrategy() {
        this((Class<R>) HasIdentifier.class);
    }

    public HasIdentifierNamingStrategy(Class<R> objectType) {
        super(objectType);
    }

    @Override
    public String getName(R hasIdentifier, ScriptGeneratorContext context) {
        return getName(hasIdentifier, context, true);
    }

    @Override
    public String getName(R hasIdentifier, ScriptGeneratorContext context, boolean identifier) {
        String name = getNameOfHasIdentifier(hasIdentifier, context, identifier);
        return identifier ? context.getDialect().getIdentifier(name, hasIdentifier) : name;
    }

    @Override
    public String getQualifiedName(R hasIdentifier, ScriptGeneratorContext context, boolean identifier) {
        Dialect dialect = identifier ? context.getDialect() : null;
        String name = getNameOfHasIdentifier(hasIdentifier, context, identifier);
        return HasIdentifierBase.getQualifiedName(dialect, context.getCatalog(),
                context.getSchema(), name, hasIdentifier);
    }

    @Override
    public String getQualifiedName(R hasIdentifier, ScriptGeneratorContext context) {
        return getQualifiedName(hasIdentifier, context, true);
    }

    protected String getNameOfHasIdentifier(R hasIdentifier, ScriptGeneratorContext context, boolean identifier) {
        return hasIdentifier.getName();
    }

    public static String md5Hex(String data) {
        return new String(encodeHex(md5(data), false));
    }
}
