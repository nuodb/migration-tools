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
package com.nuodb.tools.migration.jdbc.metamodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Schema {
    private Map<Name, Table> tables = new HashMap<Name, Table>();

    private Catalog catalog;
    private Name name;

    public Schema(Catalog catalog, Name name) {
        this.catalog = catalog;
        this.name = name;
    }

    public Schema(Catalog catalog, String name) {
        this.catalog = catalog;
        this.name = Name.valueOf(name);
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public Name getName() {
        return name;
    }

    public Table getTable(String name) {
        return getTable(Name.valueOf(name));
    }

    public Table getTable(Name name) {
        Table table = tables.get(name);
        if (table == null) {
            table = createTable(name, Table.TABLE);
        }
        return table;
    }

    public Table createTable(String name, String type) {
        return createTable(Name.valueOf(name), type);
    }

    public Table createTable(Name name, String type) {
        Table table = new Table(this, name, type);
        tables.put(name, table);
        return table;
    }

    public Collection<Table> listTables() {
        return new ArrayList<Table>(tables.values());
    }
}
