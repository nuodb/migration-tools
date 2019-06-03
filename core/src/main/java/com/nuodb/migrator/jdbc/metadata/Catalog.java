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

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.CATALOG;
import static java.lang.String.format;

public class Catalog extends IdentifiableBase implements HasSchemas {

    private Map<Identifier, Schema> schemas = newLinkedHashMap();
    private Database database;

    public Catalog() {
        super(CATALOG);
    }

    public Catalog(String name) {
        this(valueOf(name));
    }

    public Catalog(Identifier identifier) {
        super(CATALOG, identifier);
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        for (Schema schema : getSchemas()) {
            schema.setDatabase(database);
        }
        this.database = database;
    }

    public Collection<Schema> getSchemas() {
        return schemas.values();
    }

    public Schema getSchema(String name) {
        return addSchema(valueOf(name), false);
    }

    public Schema getSchema(Identifier identifier) {
        return addSchema(identifier, false);
    }

    public Schema addSchema(String name) {
        return addSchema(valueOf(name), true);
    }

    public Schema addSchema(Identifier identifier) {
        return addSchema(identifier, true);
    }

    public Schema addSchema(Schema schema) {
        schema.setDatabase(database);
        schema.setCatalog(this);
        schemas.put(schema.getIdentifier(), schema);
        return schema;
    }

    public boolean hasSchema(String name) {
        return hasSchema(valueOf(name));
    }

    public boolean hasSchema(Identifier identifier) {
        return schemas.containsKey(identifier);
    }

    public void removeSchema(Schema schema) {
        schemas.remove(schema.getIdentifier());
    }

    protected Schema addSchema(Identifier identifier, boolean create) {
        Schema schema = schemas.get(identifier);
        if (schema == null) {
            if (create) {
                addSchema(schema = new Schema(identifier));
            } else {
                throw new MetaDataException(format("Schema %s doesn't exist", identifier));
            }
        }
        return schema;
    }

    @Override
    public Collection<Table> getTables() {
        Collection<Table> tables = newArrayList();
        for (Schema schema : getSchemas()) {
            tables.addAll(schema.getTables());
        }
        return tables;
    }

    @Override
    public Collection<Sequence> getSequences() {
        Collection<Sequence> sequences = newArrayList();
        for (Schema schema : getSchemas()) {
            sequences.addAll(schema.getSequences());
        }
        return sequences;
    }

    @Override
    public Collection<UserDefinedType> getUserDefinedTypes() {
        Collection<UserDefinedType> userDefinedTypes = newArrayList();
        for (Schema schema : getSchemas()) {
            userDefinedTypes.addAll(schema.getUserDefinedTypes());
        }
        return userDefinedTypes;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        buffer.append(' ');
        buffer.append("schemas");
        buffer.append(' ');

        output(indent, buffer, getSchemas());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        Catalog catalog = (Catalog) o;

        if (database != null ? !database.equals(catalog.database) : catalog.database != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (database != null ? database.hashCode() : 0);
        return result;
    }
}
