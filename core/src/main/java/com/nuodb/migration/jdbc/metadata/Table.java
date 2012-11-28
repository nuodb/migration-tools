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
import com.google.common.collect.Sets;
import com.nuodb.migration.jdbc.dialect.DatabaseDialect;

import java.util.Collection;
import java.util.Map;

import static java.lang.String.format;

public class Table extends HasIdentifierBase {

    public static final String TABLE = "TABLE";
    public static final String VIEW = "VIEW";

    private PrimaryKey primaryKey;
    private final Map<Identifier, Column> columns = Maps.newLinkedHashMap();
    private final Map<Identifier, Index> indexes = Maps.newLinkedHashMap();
    private final Collection<ForeignKey> foreignKeys = Sets.newLinkedHashSet();
    private final Database database;
    private final Catalog catalog;
    private Schema schema;
    private String type = TABLE;

    public Table(Database database, Catalog catalog, Schema schema, String name) {
        this(database, catalog, schema, Identifier.valueOf(name));
    }

    public Table(Database database, Catalog catalog, Schema schema, Identifier identifier) {
        super(identifier);
        this.database = database;
        this.catalog = catalog;
        this.schema = schema;
    }

    public String getQualifiedName() {
        return getQualifiedName(null);
    }

    public String getQualifiedName(DatabaseDialect databaseDialect) {
        StringBuilder qualifiedName = new StringBuilder();
        if (catalog.getName() != null) {
            qualifiedName.append(catalog.getQuotedName(databaseDialect));
            qualifiedName.append('.');
        }
        if (schema.getName() != null) {
            qualifiedName.append(schema.getQuotedName(databaseDialect));
            qualifiedName.append('.');
        }
        qualifiedName.append(getQuotedName(databaseDialect));
        return qualifiedName.toString();
    }

    public void addForeignKey(ForeignKey foreignKey) {
        foreignKey.setTable(this);
        foreignKeys.add(foreignKey);
    }

    public Collection<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        primaryKey.setTable(this);
        this.primaryKey = primaryKey;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void addIndex(Index index) {
        index.setTable(this);
        indexes.put(index.getIdentifier(), index);
    }

    public Index getIndex(Identifier identifier) {
        return indexes.get(identifier);
    }

    public Collection<Index> getIndexes() {
        return indexes.values();
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

    public void setType(String type) {
        this.type = type;
    }

    public Column getColumn(String name) {
        return getColumn(name, false);
    }

    public Column createColumn(String name) {
        return getColumn(name, true);
    }

    public Column getColumn(String name, boolean create) {
        return getColumn(Identifier.valueOf(name), create);
    }

    public Column getColumn(Identifier identifier, boolean create) {
        Column column = columns.get(identifier);
        if (column == null) {
            if (create) {
                column = new Column(this, identifier);
                columns.put(identifier, column);
            } else {
                throw new MetaModelException(format("Table %s doesn't contain %s column", getName(), identifier));
            }
        }
        return column;
    }

    public Collection<Column> getColumns() {
        return columns.values();
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);
        buffer.append(' ');
        buffer.append(getType());

        outputNewLine(buffer);

        indent += INDENT;
        output(indent, buffer, "column(s)");
        buffer.append(' ');
        output(indent, buffer, getColumns());
        buffer.append(',');
        outputNewLine(buffer);

        output(indent, buffer, "primary key");
        PrimaryKey primaryKey = getPrimaryKey();
        if (primaryKey != null) {
            buffer.append(' ');
            buffer.append("column(s)");
            buffer.append(' ');
            output(indent, buffer, primaryKey.getColumns());
        }
        buffer.append(',');
        outputNewLine(buffer);

        output(indent, buffer, "index(s)");
        buffer.append(' ');
        output(indent, buffer, getIndexes());
        buffer.append(',');
        outputNewLine(buffer);

        output(indent, buffer, "foreign key(s)");
        buffer.append(' ');
        output(indent, buffer, getForeignKeys());
    }
}
