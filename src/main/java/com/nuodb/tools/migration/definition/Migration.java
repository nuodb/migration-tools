package com.nuodb.tools.migration.definition;

import java.util.ArrayList;
import java.util.List;

public class Migration {

    private List<? extends Connection> connections = new ArrayList<Connection>();
    private List<? extends Task> tasks = new ArrayList<Task>();

    public List<? extends Connection> getConnections() {
        return connections;
    }

    public void setConnections(List<? extends Connection> connections) {
        this.connections = connections;
    }

    public List<? extends Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<? extends Task> tasks) {
        this.tasks = tasks;
    }
}
