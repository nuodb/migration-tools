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

import org.hibernate.dialect.Dialect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Table extends HasObjectNameBase {
    public static final String TABLE = "TABLE";
    public static final String VIEW = "VIEW";

    private Database database;
    private Catalog catalog;
    private Schema schema;
    private String type;

    private final Map<ObjectName, Column> columns = new LinkedHashMap<ObjectName, Column>();
    private final Map<ObjectName, Index> indexes = new LinkedHashMap<ObjectName, Index>();
    private final Map<ObjectName, UniqueKey> uniqueKeys = new LinkedHashMap<ObjectName, UniqueKey>();
    private final List<Constraint> constraints = new ArrayList<Constraint>();

    public Table(Database database, Catalog catalog, Schema schema, ObjectName objectName) {
        this(database, catalog, schema, objectName, TABLE);
    }

    public Table(Database database, Catalog catalog, Schema schema, ObjectName objectName, String type) {
        super(objectName);
        this.database = database;
        this.catalog = catalog;
        this.schema = schema;
        this.type = type;
    }

    public String getQualifiedName(Dialect dialect) {
        StringBuilder qualifiedName = new StringBuilder();
        if (catalog != null && catalog.getName() != null) {
            qualifiedName.append(catalog.getQuotedName(dialect));
            qualifiedName.append('.');
        }
        if (schema != null && schema.getName() != null) {
            qualifiedName.append(schema.getQuotedName(dialect));
            qualifiedName.append('.');
        }
        qualifiedName.append(getQuotedName(dialect));
        return qualifiedName.toString();
    }

    public Database getDatabase() {
        return database;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getType() {
        return type;
    }

    public Column getColumn(String name) {
        return getColumn(ObjectName.valueOf(name));
    }

    public Column getColumn(ObjectName objectName) {
        Column column = columns.get(objectName);
        if (column == null) {
            column = createColumn(objectName);
        }
        return column;
    }

    public Column createColumn(String name) {
        return createColumn(ObjectName.valueOf(name));
    }

    public Column createColumn(ObjectName objectName) {
        Column column = new Column(this, objectName);
        columns.put(objectName, column);
        return column;
    }

    public Collection<Column> listColumns() {
        return new ArrayList<Column>(columns.values());
    }
}
