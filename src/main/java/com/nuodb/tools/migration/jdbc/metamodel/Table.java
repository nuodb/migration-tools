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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Table {
    public static final String TABLE = "TABLE";
    public static final String VIEW = "VIEW";

    private Schema schema;
    private Name name;
    private String type;

    private final Map<Name, Column> columns = new LinkedHashMap<Name, Column>();
    private final Map<Name, Index> indexes = new LinkedHashMap<Name, Index>();
    private final Map<Name, UniqueKey> uniqueKeys = new LinkedHashMap<Name, UniqueKey>();
    private final List<Constraint> constraints = new ArrayList<Constraint>();

    public Table(Schema schema, Name name) {
        this(schema, name, TABLE);
    }

    public Table(Schema schema, Name name, String type) {
        this.schema = schema;
        this.name = name;
        this.type = type;
    }

    public Schema getSchema() {
        return schema;
    }

    public Name getName() {
        return name;
    }

    public String getType() {
        return type;
    }


    public Column getColumn(String name) {
        return getColumn(Name.valueOf(name));
    }

    public Column getColumn(Name name) {
        Column column = columns.get(name);
        if (column == null) {
            column = createColumn(name);
        }
        return column;
    }

    public Column createColumn(String name) {
        return createColumn(Name.valueOf(name));
    }

    public Column createColumn(Name name) {
        Column column = new Column(this, name);
        columns.put(name, column);
        return column;
    }
}
