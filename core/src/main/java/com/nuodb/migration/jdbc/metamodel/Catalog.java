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
package com.nuodb.migration.jdbc.metamodel;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

import static com.nuodb.migration.jdbc.metamodel.Name.valueOf;
import static java.lang.String.format;

public class Catalog extends HasNameBase {

    private Map<Name, Schema> schemas = Maps.newHashMap();
    private Database database;

    public Catalog(Database database, String name) {
        this(database, valueOf(name));
    }

    public Catalog(Database database, Name name) {
        super(name);
        this.database = database;
    }

    public Database getDatabase() {
        return database;
    }

    public Schema getSchema(String name) {
        return getOrCreateSchema(valueOf(name), false);
    }

    public Schema getSchema(Name name) {
        return getOrCreateSchema(name, false);
    }

    public Schema createSchema(String name) {
        return getOrCreateSchema(valueOf(name), true);
    }

    public Schema createSchema(Name name) {
        return getOrCreateSchema(name, true);
    }

    protected Schema getOrCreateSchema(Name name, boolean create) {
        Schema schema = schemas.get(name);
        if (schema == null) {
            if (create) {
                schema = doCreateSchema(name);
            } else {
                throw new MetaModelException(format("Schema %s doesn't exist", name));
            }
        }
        return schema;
    }

    protected Schema doCreateSchema(Name name) {
        Schema schema = new Schema(database, this, name);
        schemas.put(name, schema);
        return schema;
    }

    public Collection<Schema> listSchemas() {
        return Lists.newArrayList(schemas.values());
    }
}
