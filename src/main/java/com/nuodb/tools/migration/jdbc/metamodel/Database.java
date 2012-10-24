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

import com.nuodb.tools.migration.jdbc.dialect.DatabaseDialect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {

    private Map<Name, Catalog> catalogs = new HashMap<Name, Catalog>();
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

    public Map<Name, Catalog> getCatalogs() {
        return catalogs;
    }

    public void setCatalogs(Map<Name, Catalog> catalogs) {
        this.catalogs = catalogs;
    }

    public Catalog getCatalog(String name) {
        return getCatalog(Name.valueOf(name));
    }

    public Catalog getCatalog(Name name) {
        Catalog catalog = catalogs.get(name);
        if (catalog == null) {
            catalog = createCatalog(name);
        }
        return catalog;
    }

    public Catalog createCatalog(String name) {
        return getCatalog(Name.valueOf(name));
    }

    public Catalog createCatalog(Name name) {
        Catalog catalog = new Catalog(this, name);
        catalogs.put(name, catalog);
        return catalog;
    }

    public Schema getSchema(String catalog, String schema) {
        return getSchema(Name.valueOf(catalog), Name.valueOf(schema));
    }

    public Schema getSchema(Name catalog, Name schema) {
        return getCatalog(catalog).getSchema(schema);
    }

    public Schema createSchema(String catalog, String schema) {
        return createSchema(Name.valueOf(catalog), Name.valueOf(schema));
    }

    public Schema createSchema(Name catalog, Name schema) {
        return getCatalog(catalog).createSchema(schema);
    }

    public Collection<Catalog> listCatalogs() {
        return new ArrayList<Catalog>(catalogs.values());
    }

    public Collection<Schema> listSchemas() {
        Collection<Catalog> catalogs = listCatalogs();
        List<Schema> schemas = new ArrayList<Schema>();
        for (Catalog catalog : catalogs) {
            schemas.addAll(catalog.listSchemas());
        }
        return schemas;
    }

    public Collection<Table> listTables() {
        Collection<Schema> schemas = listSchemas();
        List<Table> tables = new ArrayList<Table>();
        for (Schema schema : schemas) {
            tables.addAll(schema.listTables());
        }
        return tables;
    }
}
