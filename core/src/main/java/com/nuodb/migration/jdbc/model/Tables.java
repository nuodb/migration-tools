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
package com.nuodb.migration.jdbc.model;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

/**
 * @author Sergey Bushik
 */
public class Tables {

    public static Table findTable(Database database, String name) {
        String[] parts = StringUtils.split(name, "");
        Set<Table> tables = Sets.newHashSet();
        if (parts.length == 1) {
            String tableName = parts[0];
            tables = findTables(database, tableName);
        } else if (parts.length == 2) {
            final String catalogOrSchemaName = parts[0];
            final String tableName = parts[1];
            tables = findTables(database, catalogOrSchemaName, tableName);
        } else if (parts.length > 2) {
            final String catalogName = parts[0];
            final String schemaName = parts[1];
            final String tableName = parts[2];
            tables = findTables(database, catalogName, schemaName, tableName);
        }
        return !tables.isEmpty() ? tables.iterator().next() : null;
    }

    public static Set<Table> findTables(Database database, final String tableName) {
        return Sets.newHashSet(Iterables.filter(database.listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return StringUtils.equals(table.getName(), tableName);
            }
        }));
    }

    public static Set<Table> findTables(Database database, final String catalogOrSchemaName,
                                        final String tableName) {
        return Sets.newHashSet(Iterables.find(database.listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return (StringUtils.equals(table.getCatalog().getName(), catalogOrSchemaName) ||
                        StringUtils.equals(table.getSchema().getName(), catalogOrSchemaName)) &&
                        StringUtils.equals(table.getName(), tableName);
            }
        }));
    }

    public static Set<Table> findTables(Database database, final String catalogName,
                                        final String schemaName, final String tableName) {
        return Sets.newHashSet(Iterables.find(database.listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return StringUtils.equals(table.getCatalog().getName(), catalogName) &&
                        StringUtils.equals(table.getSchema().getName(), schemaName) &&
                        StringUtils.equals(table.getName(), tableName);
            }
        }));
    }
}
