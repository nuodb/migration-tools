package com.nuodb.tool.migration.jdbc.metamodel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Table {
    public static final String TABLE = "TABLE";
    public static final String VIEW = "VIEW";

    private Schema schema;
    private Name name;
    private String type;

    private final Map<Name, Column> columns = new LinkedHashMap<Name, Column>();
    private final Map<Name, Index> indexes = new LinkedHashMap<Name, Index>();
    private final Map<Name, UniqueKey> uniqueKeys = new LinkedHashMap<Name, UniqueKey>();
    private final List<Constraint> constraints = new ArrayList<Constraint>();

    public Table(Schema schema, Name name) {
        this(schema, name, TABLE);
    }

    public Table(Schema schema, Name name, String type) {
        this.schema = schema;
        this.name = name;
        this.type = type;
    }

    public Schema getSchema() {
        return schema;
    }

    public Name getName() {
        return name;
    }

    public String getType() {
        return type;
    }


    public Column getColumn(String name) {
        return getColumn(Name.valueOf(name));
    }

    public Column getColumn(Name name) {
        Column column = columns.get(name);
        if (column == null) {
            column = createColumn(name);
        }
        return column;
    }

    public Column createColumn(String name) {
        return createColumn(Name.valueOf(name));
    }

    public Column createColumn(Name name) {
        Column column = new Column(this, name);
        columns.put(name, column);
        return column;
    }
}
