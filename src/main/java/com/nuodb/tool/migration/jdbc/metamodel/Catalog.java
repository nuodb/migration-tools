package com.nuodb.tool.migration.jdbc.metamodel;

import java.util.HashMap;
import java.util.Map;

public class Catalog {

    private Map<Name, Schema> schemas = new HashMap<Name, Schema>();
    private Database database;
    private Name name;

    public Catalog(Database database, Name name) {
        this.database = database;
        this.name = name;
    }

    public Database getDatabase() {
        return database;
    }

    public Name getName() {
        return name;
    }

    public Schema getSchema(Name name) {
        Schema schema = schemas.get(name);
        if (schema == null) {
            schema = createSchema(name);
        }
        return schema;
    }

    protected Schema createSchema(Name name) {
        Schema schema = new Schema(this, name);
        schemas.put(name, schema);
        return schema;
    }
}
