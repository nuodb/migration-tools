package com.nuodb.tools.migration.definition;

import java.util.Collection;

public class DumpTask extends TaskBase {

    private Connection connection;
    private Collection<Table> tables;
    private Collection<Query> query;
    private Collection<Output> output;

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Collection<Table> getTables() {
        return tables;
    }

    public void setTables(Collection<Table> tables) {
        this.tables = tables;
    }

    public Collection<Query> getQuery() {
        return query;
    }

    public void setQuery(Collection<Query> query) {
        this.query = query;
    }

    public Collection<Output> getOutput() {
        return output;
    }

    public void setOutput(Collection<Output> output) {
        this.output = output;
    }
}
