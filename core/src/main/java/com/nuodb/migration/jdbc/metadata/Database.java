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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nuodb.migration.jdbc.dialect.Dialect;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.Map;

import static com.nuodb.migration.jdbc.metadata.Identifier.valueOf;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.split;

public class Database extends IndentedOutputBase implements Relational {

    private Map<Identifier, Catalog> catalogs = Maps.newLinkedHashMap();
    private DriverInfo driverInfo;
    private DatabaseInfo databaseInfo;
    private Dialect dialect;

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

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public Catalog createCatalog(String catalog) {
        return createCatalog(valueOf(catalog), true);
    }

    public Catalog createCatalog(Identifier catalog) {
        return createCatalog(catalog, true);
    }

    public Catalog getCatalog(String name) {
        return createCatalog(valueOf(name), false);
    }

    public Catalog getCatalog(Identifier identifier) {
        return createCatalog(identifier, false);
    }

    protected Catalog createCatalog(Identifier identifier, boolean create) {
        Catalog catalog = catalogs.get(identifier);
        if (catalog == null) {
            if (create) {
                catalogs.put(identifier, catalog = new Catalog(this, identifier));
            } else {
                throw new MetaDataException(format("Catalog %s doesn't exist", identifier));
            }
        }
        return catalog;
    }

    public Table findTable(String table) {
        Collection<Table> tables = findTables(table);
        if (tables.isEmpty()) {
            throw new MetaDataException(format("Can't find table %s", table));
        } else if (tables.size() > 1) {
            throw new MetaDataException(format("Multiple tables %s correspond to %s", tables, table));
        }
        return tables.iterator().next();
    }

    public Collection<Table> findTables(String name) {
        String[] parts = split(name, "");
        Collection<Table> tables = Lists.newArrayList();
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

    public Collection<Catalog> listCatalogs() {
        return catalogs.values();
    }

    public Collection<Schema> listSchemas() {
        Collection<Schema> schemas = Lists.newArrayList();
        for (Catalog catalog : listCatalogs()) {
            schemas.addAll(catalog.listSchemas());
        }
        return schemas;
    }

    public Collection<Table> listTables() {
        Collection<Schema> schemas = listSchemas();
        Collection<Table> tables = Lists.newArrayList();
        for (Schema schema : schemas) {
            tables.addAll(schema.listTables());
        }
        return tables;
    }

    public Collection<Table> listTables(String table) {
        final Identifier tableId = valueOf(table);
        return Lists.newArrayList(Iterables.filter(listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table input) {
                return ObjectUtils.equals(input.getIdentifier(), tableId);
            }
        }));
    }

    public Collection<Table> listTables(String schema, String table) {
        final Identifier schemaId = valueOf(schema);
        final Identifier tableId = valueOf(table);
        return Lists.newArrayList(Iterables.find(listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table input) {
                return ObjectUtils.equals(input.getSchema().getIdentifier(), schemaId) &&
                        ObjectUtils.equals(input.getIdentifier(), tableId);
            }
        }));
    }

    public Collection<Table> listTables(String catalog, String schema, String table) {
        final Identifier catalogId = valueOf(catalog);
        final Identifier schemaId = valueOf(schema);
        final Identifier tableId = valueOf(table);
        return Lists.newArrayList(Iterables.find(listTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table input) {
                return ObjectUtils.equals(input.getCatalog().getIdentifier(), catalogId) &&
                        ObjectUtils.equals(input.getSchema().getIdentifier(), schemaId) &&
                        ObjectUtils.equals(input.getIdentifier(), tableId);
            }
        }));
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        output(indent, buffer, "database");

        buffer.append(' ');
        buffer.append("catalogs");
        buffer.append(' ');

        output(indent, buffer, listCatalogs());
    }
}
