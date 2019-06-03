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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.spec.ConnectionSpec;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.split;

public class Database extends IdentifiableBase implements HasSchemas {

    private final Map<Identifier, Catalog> catalogs = newLinkedHashMap();

    private Dialect dialect;
    private DriverInfo driverInfo;
    private DatabaseInfo databaseInfo;
    private ConnectionSpec connectionSpec;

    public Database() {
        super(MetaDataType.DATABASE, false);
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

    public ConnectionSpec getConnectionSpec() {
        return connectionSpec;
    }

    public void setConnectionSpec(ConnectionSpec connectionSpec) {
        this.connectionSpec = connectionSpec;
    }

    public boolean hasCatalog(String name) {
        return hasCatalog(valueOf(name));
    }

    public boolean hasCatalog(Identifier identifier) {
        return catalogs.containsKey(identifier);
    }

    public Catalog getCatalog(String name) {
        return addCatalog(valueOf(name), false);
    }

    public Catalog getCatalog(Identifier identifier) {
        return addCatalog(identifier, false);
    }

    public Catalog addCatalog(String name) {
        return addCatalog(valueOf(name), true);
    }

    public Catalog addCatalog(Identifier identifier) {
        return addCatalog(identifier, true);
    }

    public Catalog addCatalog(Catalog catalog) {
        catalog.setDatabase(this);
        catalogs.put(catalog.getIdentifier(), catalog);
        return catalog;
    }

    public void removeCatalog(Catalog catalog) {
        catalogs.remove(catalog.getIdentifier());
    }

    protected Catalog addCatalog(Identifier identifier, boolean create) {
        Catalog catalog = catalogs.get(identifier);
        if (catalog == null) {
            if (create) {
                addCatalog(catalog = new Catalog(identifier));
            } else {
                throw new MetaDataException(format("Catalog %s doesn't exist", identifier));
            }
        }
        return catalog;
    }

    public Table findTable(String tableName) {
        Collection<Table> tables = findTables(tableName);
        if (tables.isEmpty()) {
            throw new MetaDataException(format("Can't find table %s", tableName));
        } else if (tables.size() > 1) {
            throw new MetaDataException(
                    format("Multiple tables %s match %s", Iterables.transform(tables, new Function<Table, String>() {
                        @Override
                        public String apply(Table table) {
                            return table.getQualifiedName(getDialect());
                        }
                    }), tableName));
        }
        return get(tables, 0);
    }

    public Collection<Table> findTables(String tableName) {
        String[] parts = split(tableName, ".");
        Collection<Table> tables = newArrayList();
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

    @Override
    public Collection<Schema> getSchemas() {
        Collection<Schema> schemas = newArrayList();
        for (Catalog catalog : getCatalogs()) {
            schemas.addAll(catalog.getSchemas());
        }
        return schemas;
    }

    @Override
    public Collection<Table> getTables() {
        Collection<Table> tables = newArrayList();
        for (Schema schema : getSchemas()) {
            tables.addAll(schema.getTables());
        }
        return tables;
    }

    @Override
    public Collection<Sequence> getSequences() {
        Collection<Sequence> sequences = newArrayList();
        for (Schema schema : getSchemas()) {
            sequences.addAll(schema.getSequences());
        }
        return sequences;
    }

    @Override
    public Collection<UserDefinedType> getUserDefinedTypes() {
        Collection<UserDefinedType> userDefinedTypes = newArrayList();
        for (Schema schema : getSchemas()) {
            userDefinedTypes.addAll(schema.getUserDefinedTypes());
        }
        return userDefinedTypes;
    }

    public Collection<Table> getTables(String tableName) {
        final Identifier tableId = valueOf(tableName);
        return newArrayList(filter(getTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return ObjectUtils.equals(table.getIdentifier(), tableId);
            }
        }));
    }

    public Collection<Table> getTables(String schemaName, String tableName) {
        final Identifier schemaId = valueOf(schemaName);
        final Identifier tableId = valueOf(tableName);
        return newArrayList(filter(getTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return ObjectUtils.equals(table.getSchema().getIdentifier(), schemaId)
                        && ObjectUtils.equals(table.getIdentifier(), tableId);
            }
        }));
    }

    public Collection<Table> getTables(String catalogName, String schemaName, String tableName) {
        final Identifier catalogId = valueOf(catalogName);
        final Identifier schemaId = valueOf(schemaName);
        final Identifier tableId = valueOf(tableName);
        return newArrayList(filter(getTables(), new Predicate<Table>() {
            @Override
            public boolean apply(Table table) {
                return ObjectUtils.equals(table.getCatalog().getIdentifier(), catalogId)
                        && ObjectUtils.equals(table.getSchema().getIdentifier(), schemaId)
                        && ObjectUtils.equals(table.getIdentifier(), tableId);
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Database database = (Database) o;

        if (connectionSpec != null ? !connectionSpec.equals(database.connectionSpec) : database.connectionSpec != null)
            return false;
        if (databaseInfo != null ? !databaseInfo.equals(database.databaseInfo) : database.databaseInfo != null)
            return false;
        if (driverInfo != null ? !driverInfo.equals(database.driverInfo) : database.driverInfo != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = driverInfo != null ? driverInfo.hashCode() : 0;
        result = 31 * result + (databaseInfo != null ? databaseInfo.hashCode() : 0);
        result = 31 * result + (connectionSpec != null ? connectionSpec.hashCode() : 0);
        return result;
    }
}
