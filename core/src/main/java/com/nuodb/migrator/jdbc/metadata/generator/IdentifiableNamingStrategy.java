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
import com.nuodb.migrator.jdbc.metadata.Identifiable;
import com.nuodb.migrator.jdbc.metadata.IdentifiableBase;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;

import static com.nuodb.migrator.utils.StringUtils.autoCase;

/**
 * @author Sergey Bushik
 */
public class IdentifiableNamingStrategy<I extends Identifiable> extends MetaDataHandlerBase
        implements NamingStrategy<I> {

    public static final char DELIMITER = '_';

    private String prefix;
    private char delimiter = DELIMITER;

    public IdentifiableNamingStrategy() {
        super(Identifiable.class);
    }

    protected IdentifiableNamingStrategy(Class<? extends I> typeClass) {
        super(typeClass);
    }

    protected IdentifiableNamingStrategy(Class<? extends MetaData> objectClass, String prefix) {
        super(objectClass);
        this.prefix = prefix;
    }

    protected IdentifiableNamingStrategy(MetaDataType objectType, String prefix) {
        super(objectType);
        this.prefix = prefix;
    }

    public IdentifiableNamingStrategy(Class<? extends MetaData> objectClass, String prefix, char delimiter) {
        super(objectClass);
        this.prefix = prefix;
        this.delimiter = delimiter;
    }

    public IdentifiableNamingStrategy(MetaDataType objectType, String prefix, char delimiter) {
        super(objectType);
        this.prefix = prefix;
        this.delimiter = delimiter;
    }

    @Override
    public String getName(I object, ScriptGeneratorManager scriptGeneratorManager, boolean normalize) {
        Dialect dialect = normalize ? scriptGeneratorManager.getTargetDialect() : null;
        String name = getNonNormalizedName(object, scriptGeneratorManager);
        return IdentifiableBase.getName(dialect, name, object);
    }

    @Override
    public String getQualifiedName(I object, ScriptGeneratorManager scriptGeneratorManager, String catalog,
            String schema, boolean normalize) {
        Dialect dialect = normalize ? scriptGeneratorManager.getTargetDialect() : null;
        String name = getNonNormalizedName(object, scriptGeneratorManager);
        return IdentifiableBase.getQualifiedName(dialect, catalog, schema, name, object);
    }

    protected String getNonNormalizedName(I object, ScriptGeneratorManager scriptGeneratorManager) {
        String prefix = getPrefix(object, scriptGeneratorManager);
        char delimiter = getDelimiter(object, scriptGeneratorManager);
        String nonPrefixedName = getNonPrefixedName(object, scriptGeneratorManager);
        return prefix != null ? autoCase(prefix, nonPrefixedName, delimiter) : nonPrefixedName;
    }

    protected String getNonPrefixedName(I object, ScriptGeneratorManager scriptGeneratorManager) {
        return object.getName();
    }

    protected String getPrefix(I object, ScriptGeneratorManager scriptGeneratorManager) {
        return getPrefix();
    }

    protected char getDelimiter(I object, ScriptGeneratorManager scriptGeneratorManager) {
        return getDelimiter();
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }
}
