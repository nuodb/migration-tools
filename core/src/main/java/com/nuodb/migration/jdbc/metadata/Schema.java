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

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

import static com.nuodb.migration.jdbc.metadata.Identifier.valueOf;
import static java.lang.String.format;

public class Schema extends HasIdentifierBase {

    private Map<Identifier, Table> tables = Maps.newLinkedHashMap();

    private Database database;
    private Catalog catalog;

    public Schema(Database database, Catalog catalog, String name) {
        this(database, catalog, valueOf(name));
    }

    public Schema(Database database, Catalog catalog, Identifier identifier) {
        super(identifier);
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
        return createTable(valueOf(name), false);
    }

    public Table getTable(Identifier identifier) {
        return createTable(identifier, false);
    }

    public Table createTable(String name) {
        return createTable(valueOf(name), true);
    }

    protected Table createTable(Identifier identifier, boolean create) {
        Table table = tables.get(identifier);
        if (table == null) {
            if (create) {
                table = new Table(database, catalog, this, identifier);
                tables.put(identifier, table);
            } else {
                throw new MetaModelException(format("Table %s doesn't exist", identifier));
            }
        }
        return table;
    }

    public Collection<Table> listTables() {
        return tables.values();
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        buffer.append(' ');
        buffer.append("table(s)");
        buffer.append(' ');

        output(indent, buffer, listTables());
    }
}
