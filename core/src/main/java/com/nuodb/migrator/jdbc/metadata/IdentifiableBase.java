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
package com.nuodb.migrator.jdbc.metadata;

import com.nuodb.migrator.jdbc.dialect.Dialect;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * @author Sergey Bushik
 */
public class IdentifiableBase extends IndentedBase implements Identifiable {

    private boolean qualified;
    private Identifier identifier;
    private MetaDataType objectType;

    public IdentifiableBase(MetaDataType objectType) {
        this(objectType, Identifier.EMPTY);
    }

    public IdentifiableBase(MetaDataType objectType, boolean qualified) {
        this.objectType = objectType;
        this.qualified = qualified;
    }

    public IdentifiableBase(MetaDataType objectType, String name) {
        this(objectType, name, false);
    }

    public IdentifiableBase(MetaDataType objectType, String name, boolean qualified) {
        this(objectType, valueOf(name), qualified);
    }

    public IdentifiableBase(MetaDataType objectType, Identifier identifier) {
        this(objectType, identifier, false);
    }

    public IdentifiableBase(MetaDataType objectType, Identifier identifier, boolean qualified) {
        this.qualified = qualified;
        this.identifier = identifier;
        this.objectType = objectType;
    }

    @Override
    public String getName() {
        return identifier != null ? identifier.value() : null;
    }

    @Override
    public void setName(String name) {
        this.identifier = valueOf(name);
    }

    @Override
    public String getName(Dialect dialect) {
        return getName(dialect, getName(), this);
    }

    @Override
    public String getQualifiedName() {
        return getQualifiedName(null);
    }

    @Override
    public String getQualifiedName(Dialect dialect) {
        return getQualifiedName(dialect, null, null, getName(), this);
    }

    @Override
    public String getQualifiedName(String catalog, String schema) {
        return getQualifiedName(null, catalog, schema, this);
    }

    @Override
    public String getQualifiedName(Dialect dialect, String catalog, String schema) {
        return getQualifiedName(dialect, catalog, schema, this);
    }

    @Override
    public boolean isQualified() {
        return qualified;
    }

    public void setQualified(boolean qualified) {
        this.qualified = qualified;
    }

    @Override
    public Identifier getIdentifier() {
        return identifier;
    }

    @Override
    public void setIdentifier(Identifier identifier) {
        this.identifier = identifier;
    }

    @Override
    public MetaDataType getObjectType() {
        return objectType;
    }

    public static String getName(Dialect dialect, String name, Identifiable identifiable) {
        return dialect != null ? dialect.getIdentifier(name, identifiable) : name;
    }

    public static String getQualifiedName(Dialect dialect, String catalog, String schema, Identifiable identifiable) {
        return getQualifiedName(dialect, catalog, schema, identifiable.getName(), identifiable);
    }

    public static String getQualifiedName(Dialect dialect, String catalog, String schema, String name,
            Identifiable identifiable) {
        return getQualifiedName(dialect, newArrayList(catalog, schema), name, identifiable);
    }

    public static String getQualifiedName(Dialect dialect, Collection<String> qualifiers, String name,
            Identifiable identifiable) {
        if (identifiable == null || identifiable.isQualified()) {
            Collection<String> parts = newArrayList();
            for (String qualifier : qualifiers) {
                if (qualifier != null) {
                    parts.add(dialect != null ? dialect.getIdentifier(qualifier, null) : qualifier);
                }
            }
            if (name != null) {
                parts.add(dialect != null ? dialect.getIdentifier(name, null) : name);
            }
            return join(parts, '.');
        } else {
            return getName(dialect, name, identifiable);
        }
    }

    @Override
    public int compareTo(Identifiable identifiable) {
        return identifier.compareTo(identifiable.getIdentifier());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object)
            return true;
        if (!(object instanceof Identifiable))
            return false;
        Identifiable identifiable = (Identifiable) object;
        if (identifier != null ? !identifier.equals(identifiable.getIdentifier())
                : identifiable.getIdentifier() != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return identifier != null ? identifier.hashCode() : 0;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        output(indent, buffer, identifier != null ? identifier.value() : "");
    }
}
