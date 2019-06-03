/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 * <p/>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p/>
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of NuoDB, Inc. nor the names of its contributors may
 * be used to endorse or promote products derived from this software
 * without specific prior written permission.
 * <p/>
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
package com.nuodb.migrator.jdbc.metadata;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static java.lang.String.format;

public class Schema extends IdentifiableBase implements HasTables {

    private Database database;
    private Catalog catalog;
    private final Collection<Sequence> sequences = newArrayList();
    private final Map<Identifier, UserDefinedType> userDefinedTypes = newLinkedHashMap();
    private final Map<Identifier, Table> tables = newLinkedHashMap();

    public Schema() {
        super(SCHEMA);
    }

    public Schema(String name) {
        this(valueOf(name));
    }

    public Schema(Identifier identifier) {
        super(SCHEMA, identifier);
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        for (Table table : getTables()) {
            table.setDatabase(database);
        }
        this.database = database;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        for (Table table : getTables()) {
            table.setDatabase(catalog.getDatabase());
            table.setCatalog(catalog);
        }
        this.catalog = catalog;
    }

    public void addSequence(Sequence sequence) {
        if (sequences.add(sequence)) {
            sequence.setSchema(this);
        }
    }

    public void removeSequence(Sequence sequence) {
        if (sequences.remove(sequence)) {
            sequence.setSchema(null);
        }
    }

    public Sequence getSequence(String name) {
        for (Sequence sequence : sequences) {
            if (sequence.getName().equals(name)) {
                return sequence;
            }
        }
        return null;
    }

    @Override
    public Collection<Sequence> getSequences() {
        return sequences;
    }

    public void addUserDefinedType(UserDefinedType userDefinedType) {
        userDefinedType.setSchema(this);
        userDefinedTypes.put(userDefinedType.getIdentifier(), userDefinedType);
    }

    public UserDefinedType getUserDefinedType(String name) {
        return getUserDefinedType(valueOf(name));
    }

    public UserDefinedType getUserDefinedType(Identifier identifier) {
        return userDefinedTypes.get(identifier);
    }

    @Override
    public Collection<UserDefinedType> getUserDefinedTypes() {
        return userDefinedTypes.values();
    }

    public Table getTable(String name) {
        return addTable(valueOf(name), false);
    }

    public Table getTable(Identifier identifier) {
        return addTable(identifier, false);
    }

    public Table addTable(String name) {
        return addTable(valueOf(name), true);
    }

    public Table addTable(Identifier identifier) {
        return addTable(identifier, true);
    }

    public void addTable(Table table) {
        table.setDatabase(catalog != null ? catalog.getDatabase() : null);
        table.setCatalog(catalog);
        table.setSchema(this);
        tables.put(table.getIdentifier(), table);
    }

    public boolean hasTable(String name) {
        return hasTable(valueOf(name));
    }

    public boolean hasTable(Identifier identifier) {
        return tables.containsKey(identifier);
    }

    protected Table addTable(Identifier identifier, boolean create) {
        Table table = tables.get(identifier);
        if (table == null) {
            if (create) {
                addTable(table = new Table(identifier));
            } else {
                throw new MetaDataException(format("Table %s doesn't exist", identifier));
            }
        }
        return table;
    }

    public void removeTable(Table table) {
        tables.remove(table.getIdentifier());
    }

    @Override
    public Collection<Table> getTables() {
        return tables.values();
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        buffer.append(' ');
        buffer.append("tables");
        buffer.append(' ');

        output(indent, buffer, getTables());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        Schema schema = (Schema) o;

        if (database != null ? !database.equals(schema.database) : schema.database != null)
            return false;
        if (catalog != null ? !catalog.equals(schema.catalog) : schema.catalog != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (database != null ? database.hashCode() : 0);
        result = 31 * result + (catalog != null ? catalog.hashCode() : 0);
        return result;
    }
}
