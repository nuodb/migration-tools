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
package com.nuodb.migration.jdbc.model;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

import static com.nuodb.migration.jdbc.model.Identifier.valueOf;
import static java.lang.String.format;

public class Catalog extends HasIdentifierBase {

    private Map<Identifier, Schema> schemas = Maps.newLinkedHashMap();
    private Database database;

    public Catalog(Database database, String name) {
        this(database, valueOf(name));
    }

    public Catalog(Database database, Identifier identifier) {
        super(identifier);
        this.database = database;
    }

    public Database getDatabase() {
        return database;
    }

    public Schema getSchema(String name) {
        return createSchema(valueOf(name), false);
    }

    public Schema getSchema(Identifier identifier) {
        return createSchema(identifier, false);
    }

    public Schema createSchema(String name) {
        return createSchema(valueOf(name), true);
    }

    public Schema createSchema(Identifier identifier) {
        return createSchema(identifier, true);
    }

    protected Schema createSchema(final Identifier identifier, boolean create) {
        Schema schema = schemas.get(identifier);
        if (schema == null) {
            if (create) {
                schemas.put(identifier, schema = new Schema(database, this, identifier));
            } else {
                throw new ModelException(format("Schema %s doesn't exist", identifier));
            }
        }
        return schema;
    }

    public Collection<Schema> listSchemas() {
        return schemas.values();
    }
}
