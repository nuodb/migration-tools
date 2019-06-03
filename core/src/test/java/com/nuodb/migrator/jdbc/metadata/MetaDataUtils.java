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

import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;

/**
 * @author Sergey Bushik
 */
public class MetaDataUtils {

    public static Database createDatabase() {
        return new Database();
    }

    public static Catalog createCatalog(String catalog) {
        return createDatabase().addCatalog(catalog);
    }

    public static Schema createSchema(String catalog, String schema) {
        return createCatalog(catalog).addSchema(schema);
    }

    public static Table createTable(String catalog, String schema, String table) {
        return createSchema(catalog, schema).addTable(table);
    }

    public static Column createColumn(String catalog, String schema, String table, String column) {
        return createTable(catalog, schema, table).addColumn(column);
    }

    public static Sequence createSequence(String catalog, String schema, String table, String column) {
        return createSequence(null, catalog, schema, table, column);
    }

    public static Sequence createSequence(String name, String catalog, String schema, String table, String column) {
        Sequence sequence = new Sequence(valueOf(name));
        Column c = createColumn(catalog, schema, table, column);
        c.setSequence(sequence);
        c.getTable().getSchema().addSequence(sequence);
        return sequence;
    }

    public static Index createIndex(String name, Collection<Column> columns, boolean unique) {
        Index index = new Index(valueOf(name));
        index.setUnique(unique);
        int position = 0;
        Table table = null;
        for (Column column : columns) {
            if (table == null) {
                table = column.getTable();
            }
            index.addColumn(column, position++);
        }
        index.setTable(table);
        return index;
    }

    public static ForeignKey createForeignKey(String name, Collection<Column> primaryColumns,
            Collection<Column> foreignColumns) {
        Table primaryTable = null;
        Table foreignTable = null;
        int position = 0;
        ForeignKey foreignKey = new ForeignKey(valueOf(name));
        for (Column primaryColumn : primaryColumns) {
            if (primaryTable == null) {
                primaryTable = primaryColumn.getTable();
            }
            Column foreignColumn = get(foreignColumns, position);
            if (foreignTable == null) {
                foreignTable = foreignColumn.getTable();
            }
            foreignKey.addReference(primaryColumn, foreignColumn, position);
            position++;
        }
        foreignKey.setPrimaryTable(primaryTable);
        foreignKey.setForeignTable(foreignTable);
        return foreignKey;
    }
}
