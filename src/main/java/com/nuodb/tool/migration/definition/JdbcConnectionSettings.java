package com.nuodb.tool.migration.definition;

public class JdbcConnectionSettings {
    private String catalog;
    private String schema;

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
