package com.nuodb.tool.migration.definition;

import java.util.List;

public class Migration {

    private List<Connection> connections;
    private List<Transformation> transformations;

    public List<Connection> getConnections() {
        return connections;
    }

    public void setConnections(List<Connection> connections) {
        this.connections = connections;
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<Transformation> transformations) {
        this.transformations = transformations;
    }
}
