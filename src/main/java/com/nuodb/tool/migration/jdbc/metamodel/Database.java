package com.nuodb.tool.migration.jdbc.metamodel;

import java.util.HashMap;
import java.util.Map;

public class Database {

    private Map<Name, Catalog> catalogs = new HashMap<Name, Catalog>();
    private DatabaseInfo databaseInfo;
    private DriverInfo driverInfo;

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
}
