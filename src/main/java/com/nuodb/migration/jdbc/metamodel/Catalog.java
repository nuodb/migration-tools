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

public class Catalog extends HasNameBase {

    private Map<Name, Schema> schemas = Maps.newHashMap();
    private Database database;

    public Catalog(Database database, String name) {
        this(database, Name.valueOf(name));
    }

    public Catalog(Database database, Name name) {
        super(name);
        this.database = database;
    }

    public Database getDatabase() {
        return database;
    }

    public Schema getSchema(String name) {
        return getSchema(name, false);
    }

    public Schema getSchema(Name name) {
        return getSchema(name, false);
    }

    public Schema getSchema(String name, boolean create) {
        return getSchema(Name.valueOf(name), create);
    }

    public Schema getSchema(Name name, boolean create) {
        Schema schema = schemas.get(name);
        if (schema == null) {
            if (create) {
                schema = createSchema(name);
            } else {
                throw new MetaModelException(String.format("Schema %s doesn't exist", name));
            }
        }
        return schema;
    }

    public Schema createSchema(Name name) {
        Schema schema = new Schema(database, this, name);
        schemas.put(name, schema);
        return schema;
    }

    public Collection<Schema> listSchemas() {
        return Lists.newArrayList(schemas.values());
    }
}
