package com.nuodb.tool.migration.jdbc.metamodel;

import java.util.HashMap;
import java.util.Map;

public class Schema {
    private Map<Name, Table> tables = new HashMap<Name, Table>();

    private Catalog catalog;
    private Name name;

    public Schema(Catalog catalog, Name name) {
        this.catalog = catalog;
        this.name = name;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public Name getName() {
        return name;
    }

    public Table getTable(String name) {
        return getTable(Name.valueOf(name));
    }

    public Table getTable(Name name) {
        Table table = tables.get(name);
        if (table == null) {
            table = createTable(name, Table.TABLE);
        }
        return table;
    }

    public Table createTable(String name, String type) {
        return createTable(Name.valueOf(name), type);
    }

    public Table createTable(Name name, String type) {
        Table table = new Table(this, name, type);
        tables.put(name, table);
        return table;
    }
}
