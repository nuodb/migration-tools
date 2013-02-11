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
package com.nuodb.migration.jdbc.metadata;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

import static com.nuodb.migration.jdbc.metadata.Identifier.valueOf;
import static java.lang.String.format;

public class Catalog extends IdentifiableBase implements HasTables {

    private Map<Identifier, Schema> schemas = Maps.newLinkedHashMap();
    private Database database;

    public Catalog(String name) {
        this(valueOf(name));
    }

    public Catalog(Identifier identifier) {
        super(MetaDataType.CATALOG, identifier);
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

    public Schema getSchema(String schemaName) {
        return addSchema(valueOf(schemaName), false);
    }

    public Schema getSchema(Identifier schemaId) {
        return addSchema(schemaId, false);
    }

    public Schema addSchema(String schemaName) {
        return addSchema(valueOf(schemaName), true);
    }

    public Schema addSchema(Identifier schemaId) {
        return addSchema(schemaId, true);
    }

    public Schema addSchema(Schema schema) {
        schema.setDatabase(database);
        schema.setCatalog(this);
        schemas.put(schema.getIdentifier(), schema);
        return schema;
    }

    public boolean hasSchema(String schemaName) {
        return hasSchema(valueOf(schemaName));
    }

    public boolean hasSchema(Identifier schemaId) {
        return schemas.containsKey(schemaId);
    }

    public void removeSchema(Schema schema) {
        schemas.remove(schema.getIdentifier());
    }

    protected Schema addSchema(Identifier schemaId, boolean create) {
        Schema schema = schemas.get(schemaId);
        if (schema == null) {
            if (create) {
                addSchema(schema = new Schema(schemaId));
            } else {
                throw new MetaDataException(format("Schema %s doesn't exist", schemaId));
            }
        }
        return schema;
    }

    @Override
    public Collection<Table> getTables() {
        Collection<Schema> schemas = getSchemas();
        Collection<Table> tables = Lists.newArrayList();
        for (Schema schema : schemas) {
            tables.addAll(schema.getTables());
        }
        return tables;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Catalog catalog = (Catalog) o;

        if (database != null ? !database.equals(catalog.database) : catalog.database != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (database != null ? database.hashCode() : 0);
        return result;
    }
}
