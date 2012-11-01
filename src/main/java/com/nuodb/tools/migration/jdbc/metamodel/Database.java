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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nuodb.tools.migration.jdbc.dialect.DatabaseDialect;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Database {

    private Map<Name, Catalog> catalogs = Maps.newHashMap();
    private DriverInfo driverInfo;
    private DatabaseInfo databaseInfo;
    private DatabaseDialect databaseDialect;

    public DriverInfo getDriverInfo() {
        return driverInfo;
    }

    public void setDriverInfo(DriverInfo driverInfo) {
        this.driverInfo = driverInfo;
    }

    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }

    public void setDatabaseInfo(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;
    }

    public DatabaseDialect getDatabaseDialect() {
        return databaseDialect;
    }

    public void setDatabaseDialect(DatabaseDialect databaseDialect) {
        this.databaseDialect = databaseDialect;
    }

    public Catalog createCatalog(String catalog) {
        return getCatalog(catalog, true);
    }

    public Schema createSchema(String catalog, String schema) {
        return getCatalog(catalog, true).getSchema(schema, true);
    }

    public Catalog getCatalog(String name) {
        return getCatalog(Name.valueOf(name));
    }

    public Catalog getCatalog(Name name) {
        return getCatalog(name, false);
    }

    public Catalog getCatalog(String catalog, boolean create) {
        return getCatalog(Name.valueOf(catalog), create);
    }

    public Catalog getCatalog(Name name, boolean create) {
        Catalog catalog = catalogs.get(name);
        if (catalog == null) {
            if (create) {
                catalog = createCatalog(name);
            } else {
                throw new MetaModelException(String.format("Catalog %s doesn't exist", name));
            }
        }
        return catalog;
    }

    private Catalog createCatalog(Name name) {
        Catalog catalog = new Catalog(this, name);
        catalogs.put(name, catalog);
        return catalog;
    }

    public List<Catalog> listCatalogs() {
        return Lists.newArrayList(catalogs.values());
    }

    public List<Schema> listSchemas() {
        Collection<Catalog> catalogs = listCatalogs();
        List<Schema> schemas = Lists.newArrayList();
        for (Catalog catalog : catalogs) {
            schemas.addAll(catalog.listSchemas());
        }
        return schemas;
    }

    public List<Table> listTables() {
        Collection<Schema> schemas = listSchemas();
        List<Table> tables = Lists.newArrayList();
        for (Schema schema : schemas) {
            tables.addAll(schema.listTables());
        }
        return tables;
    }

    public Table findTable(String table) {
        List<Table> tables = findTables(table);
        if (tables.isEmpty()) {
            throw new MetaModelException(String.format("Table %s doesn't exist", table));
        } else if (tables.size() > 1) {
            throw new MetaModelException(String.format("Multiple tables %s correspond to %s", tables, table));
        }
        return tables.get(0);
    }

    public List<Table> findTables(String table) {
        String[] parts = StringUtils.split(table, ".");
        List<Table> tables = Lists.newArrayList();
        if (parts.length == 1) {
            tables.addAll(listTables(parts[0]));
        } else if (parts.length == 2) {
            tables.addAll(listTables(parts[0], parts[1]));
            tables.addAll(listTables(parts[0], null, parts[1]));
        } else if (parts.length > 2) {
            tables.addAll(listTables(parts[0], parts[1], parts[2]));
        }
        return tables;
    }

    public List<Table> listTables(final String tableName) {
        return Lists.newArrayList(Iterables.filter(listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return StringUtils.equals(table.getName(), tableName);
            }
        }));
    }

    public List<Table> listTables(final String schemaName, final String tableName) {
        return Lists.newArrayList(Iterables.find(listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return StringUtils.equals(table.getSchema().getName(), schemaName) &&
                        StringUtils.equals(table.getName(), tableName);
            }
        }));
    }

    public List<Table> listTables(final String catalogName, final String schemaName, final String tableName) {
        return Lists.newArrayList(Iterables.find(listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return StringUtils.equals(table.getCatalog().getName(), catalogName) &&
                        StringUtils.equals(table.getSchema().getName(), schemaName) &&
                        StringUtils.equals(table.getName(), tableName);
            }
        }));
    }
}
