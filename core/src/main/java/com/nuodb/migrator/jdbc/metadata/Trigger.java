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
package com.nuodb.migrator.jdbc.metadata;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import com.nuodb.migrator.jdbc.dialect.Dialect;

public class Trigger extends IdentifiableBase implements Constraint {

    public static final String TRIGGER = "TRIGGER";

    private Column column;
    private Database database;
    private Catalog catalog;
    private Schema schema;
    private Table table;
    
    private String comment;

    private Map<Integer, Column> columns = Maps.newTreeMap();
    
    public Trigger(String name) {
        this(valueOf(name));
    }

    public Trigger(Identifier identifier) {
        super(MetaDataType.TRIGGER, identifier, true);
    }
    
    @Override
    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }


    @Override
    public Collection<Column> getColumns() {
        return newArrayList(columns.values());
    }

    public void addColumn(Column column, int position) {
        columns.put(position, column);
    }
    
    public void addColumn(Column column) {
        columns.put(columns.size(),column);
    }
    
    @Override
    public String getQualifiedName(Dialect dialect) {
        return getQualifiedName(dialect, getCatalog() != null ? getCatalog().getName() : null,
                getSchema() != null ? getSchema().getName() : null);
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

    
    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Trigger table = (Trigger) o;

        if (database != null ? !database.equals(table.database) : table.database != null) return false;
        if (catalog != null ? !catalog.equals(table.catalog) : table.catalog != null) return false;
        if (schema != null ? !schema.equals(table.schema) : table.schema != null) return false;

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

    /**
     * @return the column
     */
    public Column getColumn() {
        return column;
    }

    /**
     * @param column the column to set
     */
    public void setColumn(Column column) {
        this.column = column;
    }
}
