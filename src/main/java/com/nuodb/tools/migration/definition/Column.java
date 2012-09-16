package com.nuodb.tools.migration.definition;

public class Column {

    protected String name;
    protected boolean include;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isInclude() {
        return include;
    }

    public void setInclude(boolean include) {
        this.include = include;
    }
}
