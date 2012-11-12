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
import static com.nuodb.migration.jdbc.metamodel.Table.TABLE;

public class Schema extends HasNameBase {
    private Map<Name, Table> tables = Maps.newHashMap();

    private Database database;
    private Catalog catalog;

    public Schema(Database database, Catalog catalog, String name) {
        this(database, catalog, valueOf(name));
    }

    public Schema(Database database, Catalog catalog, Name name) {
        super(name);
        this.database = database;
        this.catalog = catalog;
    }

    public Database getDatabase() {
        return database;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public Table getTable(String name) {
        return getOrCreateTable(valueOf(name), TABLE, false);
    }

    public Table getTable(Name name) {
        return getOrCreateTable(name, TABLE, false);
    }

    public Table createTable(String name, String type) {
        return getOrCreateTable(valueOf(name), type, true);
    }

    protected Table getOrCreateTable(Name name, String type, boolean create) {
        Table table = tables.get(name);
        if (table == null && create) {
            table = doCreateTable(name, type);
        } else {
            throw new MetaModelException(String.format("Table %s doesn't exist", name));
        }
        return table;
    }

    protected Table doCreateTable(Name name, String type) {
        Table table = new Table(database, catalog, this, name, type);
        tables.put(name, table);
        return table;
    }

    public Collection<Table> listTables() {
        return Lists.newArrayList(tables.values());
    }
}
