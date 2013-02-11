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

import com.google.common.base.Function;
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

public class Database extends IndentedBase implements HasTables {

    private final Map<Identifier, Catalog> catalogs = Maps.newLinkedHashMap();
    private DriverInfo driverInfo;
    private DatabaseInfo databaseInfo;
    private Dialect dialect;

    @Override
    public MetaDataType getObjectType() {
        return MetaDataType.DATABASE;
    }

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

    public boolean hasCatalog(String catalogName) {
        return hasCatalog(valueOf(catalogName));
    }

    public boolean hasCatalog(Identifier catalogId) {
        return catalogs.containsKey(catalogId);
    }

    public Catalog getCatalog(String catalogName) {
        return addCatalog(valueOf(catalogName), false);
    }

    public Catalog getCatalog(Identifier catalogId) {
        return addCatalog(catalogId, false);
    }

    public Catalog addCatalog(String catalogName) {
        return addCatalog(valueOf(catalogName), true);
    }

    public Catalog addCatalog(Identifier catalogId) {
        return addCatalog(catalogId, true);
    }

    public Catalog addCatalog(Catalog catalog) {
        catalog.setDatabase(this);
        catalogs.put(catalog.getIdentifier(), catalog);
        return catalog;
    }

    public void removeCatalog(Catalog catalog) {
        catalogs.remove(catalog.getIdentifier());
    }

    protected Catalog addCatalog(Identifier catalogId, boolean create) {
        Catalog catalog = catalogs.get(catalogId);
        if (catalog == null) {
            if (create) {
                addCatalog(catalog = new Catalog(catalogId));
            } else {
                throw new MetaDataException(format("Catalog %s doesn't exist", catalogId));
            }
        }
        return catalog;
    }

    public Table findTable(String tableName) {
        Collection<Table> tables = findTables(tableName);
        if (tables.isEmpty()) {
            throw new MetaDataException(format("Can't find table %s", tableName));
        } else if (tables.size() > 1) {
            throw new MetaDataException(format("Multiple tables %s match %s",
                    Iterables.transform(tables,
                            new Function<Table, String>() {
                                @Override
                                public String apply(Table table) {
                                    return table.getQualifiedName(getDialect());
                                }
                            }), tableName));
        }
        return tables.iterator().next();
    }

    public Collection<Table> findTables(String tableName) {
        String[] parts = split(tableName, "");
        Collection<Table> tables = Lists.newArrayList();
        if (parts.length == 1) {
            tables.addAll(getTables(parts[0]));
        } else if (parts.length == 2) {
            tables.addAll(getTables(parts[0], parts[1]));
            tables.addAll(getTables(parts[0], null, parts[1]));
        } else if (parts.length > 2) {
            tables.addAll(getTables(parts[0], parts[1], parts[2]));
        }
        return tables;
    }

    public Collection<Catalog> getCatalogs() {
        return catalogs.values();
    }

    public Collection<Schema> getSchemas() {
        Collection<Schema> schemas = Lists.newArrayList();
        for (Catalog catalog : getCatalogs()) {
            schemas.addAll(catalog.getSchemas());
        }
        return schemas;
    }

    public Collection<Table> getTables() {
        Collection<Schema> schemas = getSchemas();
        Collection<Table> tables = Lists.newArrayList();
        for (Schema schema : schemas) {
            tables.addAll(schema.getTables());
        }
        return tables;
    }

    public Collection<Table> getTables(String tableName) {
        final Identifier tableId = valueOf(tableName);
        return Lists.newArrayList(Iterables.filter(getTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table input) {
                return ObjectUtils.equals(input.getIdentifier(), tableId);
            }
        }));
    }

    public Collection<Table> getTables(String schemaName, String tableName) {
        final Identifier schemaId = valueOf(schemaName);
        final Identifier tableId = valueOf(tableName);
        return Lists.newArrayList(Iterables.find(getTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table input) {
                return ObjectUtils.equals(input.getSchema().getIdentifier(), schemaId) &&
                        ObjectUtils.equals(input.getIdentifier(), tableId);
            }
        }));
    }

    public Collection<Table> getTables(String catalogName, String schemaName, String tableName) {
        final Identifier catalogId = valueOf(catalogName);
        final Identifier schemaId = valueOf(schemaName);
        final Identifier tableId = valueOf(tableName);
        return Lists.newArrayList(Iterables.find(getTables(), new Predicate<Table>() {
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

        output(indent, buffer, getCatalogs());
    }
}
