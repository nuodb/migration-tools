/**
 * Copyright (c) 2015, NuoDB, Inc.
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
package com.nuodb.migrator.jdbc.metadata;

import com.google.common.primitives.Ints;
import com.nuodb.migrator.jdbc.dialect.Dialect;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.*;
import static com.google.common.collect.Sets.newTreeSet;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.utils.Collections.addIgnoreNull;
import static java.lang.String.format;
import static java.util.Collections.singleton;

public class Table extends IdentifiableBase {

    public static final String TABLE = "TABLE";
    public static final String VIEW = "VIEW";
    public static final String ALIAS = "ALIAS";
    public static final String SYNONYM = "SYNONYM";

    private Database database;
    private Catalog catalog;
    private Schema schema;

    private Map<Identifier, Column> columns = newLinkedHashMap();
    private Map<Identifier, Index> indexes = newLinkedHashMap();
    private Collection<ForeignKey> foreignKeys = newLinkedHashSet();

    private Collection<Trigger> triggers = newHashSet();
    private PrimaryKey primaryKey;
    private Collection<Check> checks = newHashSet();

    private String type = TABLE;
    private String comment;

    public Table() {
        super(MetaDataType.TABLE);
    }

    public Table(String name) {
        this(valueOf(name));
    }

    public Table(Identifier identifier) {
        super(MetaDataType.TABLE, identifier, true);
    }

    @Override
    public String getQualifiedName(Dialect dialect) {
        return getQualifiedName(dialect, getCatalog() != null ? getCatalog().getName() : null,
                getSchema() != null ? getSchema().getName() : null);
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

    public Index addIndex(Index index) {
        index.setTable(this);
        indexes.put(index.getIdentifier(), index);
        return index;
    }

    public boolean hasIndex(Identifier identifier) {
        return indexes.containsKey(identifier);
    }

    public Index getIndex(Identifier identifier) {
        return indexes.get(identifier);
    }

    public Check addCheck(Check check) {
        check.setTable(this);
        checks.add(check);
        return check;
    }

    public Collection<Index> getIndexes() {
        return indexes.values();
    }

    public Collection<Sequence> getSequences() {
        Collection<Sequence> sequences = newArrayList();
        for (Column column : getColumns()) {
            addIgnoreNull(sequences, column.getSequence());
        }
        return sequences;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Column getColumn(String name) {
        return getColumn(valueOf(name));
    }

    public Trigger addTrigger(Trigger trigger) {
        trigger.setTable(this);
        triggers.add(trigger);
        return trigger;
    }

    public Collection<Trigger> getTriggers() {
        return triggers;
    }

    public void setTriggers(Collection<Trigger> triggers) {
        this.triggers = triggers;
    }

    public Column getColumn(Identifier identifier) {
        return addColumn(identifier, false);
    }

    public boolean hasColumn(String name) {
        return hasColumn(valueOf(name));
    }

    public boolean hasColumn(Identifier identifier) {
        return columns.containsKey(identifier);
    }

    public Column addColumn(String name) {
        return addColumn(valueOf(name), true);
    }

    public Column addColumn(Identifier identifier) {
        return addColumn(identifier, true);
    }

    public Column addColumn(Identifier identifier, boolean create) {
        Column column = columns.get(identifier);
        if (column == null) {
            if (create) {
                column = addColumn(identifier, columns.size() + 1);
            } else {
                throw new MetaDataException(format("Table %s doesn't have %s column", getName(), identifier));
            }
        }
        return column;
    }

    public Column addColumn(Identifier identifier, int position) {
        Column column = new Column(identifier);
        column.setPosition(position);
        return addColumn(column);
    }

    public Column addColumn(Column column) {
        column.setTable(this);
        columns.put(column.getIdentifier(), column);
        return column;
    }

    public Collection<Column> getColumns() {
        Collection<Column> columns = newTreeSet(new Comparator<Column>() {
            @Override
            public int compare(Column o1, Column o2) {
                int names = o1.getName().compareToIgnoreCase(o2.getName());
                int positions = Ints.compare(o1.getPosition(), o2.getPosition());
                return positions != 0 ? positions : names;
            }
        });
        columns.addAll(this.columns.values());
        return columns;
    }

    public Collection<Check> getChecks() {
        return checks;
    }

    public void setChecks(Collection<Check> checks) {
        this.checks = checks;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);
        buffer.append(' ');
        buffer.append(getType());

        outputNewLine(buffer);

        indent += INDENT;
        output(indent, buffer, "columns");
        buffer.append(' ');
        output(indent, buffer, getColumns());

        PrimaryKey primaryKey = getPrimaryKey();
        if (primaryKey != null) {
            buffer.append(',');
            outputNewLine(buffer);
            output(indent, buffer, "primary key");
            buffer.append(' ');
            output(indent, buffer, singleton(primaryKey));
        }

        Collection<Index> indexes = getIndexes();
        if (indexes != null && !indexes.isEmpty()) {
            buffer.append(',');
            outputNewLine(buffer);
            output(indent, buffer, "indexes");
            buffer.append(' ');
            output(indent, buffer, indexes);
        }

        Collection<ForeignKey> foreignKeys = getForeignKeys();
        if (foreignKeys != null && !foreignKeys.isEmpty()) {
            buffer.append(',');
            outputNewLine(buffer);
            output(indent, buffer, "foreign keys");
            buffer.append(' ');
            output(indent, buffer, getForeignKeys());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        Table table = (Table) o;

        if (database != null ? !database.equals(table.database) : table.database != null)
            return false;
        if (catalog != null ? !catalog.equals(table.catalog) : table.catalog != null)
            return false;
        if (schema != null ? !schema.equals(table.schema) : table.schema != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (database != null ? database.hashCode() : 0);
        result = 31 * result + (catalog != null ? catalog.hashCode() : 0);
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        return result;
    }
}
