package com.nuodb.tools.migration.definition.xml.handlers;

import com.nuodb.tools.migration.definition.Connection;
import com.nuodb.tools.migration.definition.Migration;
import com.nuodb.tools.migration.definition.Task;
import com.nuodb.tools.migration.definition.xml.XmlConstants;
import com.nuodb.tools.migration.definition.xml.XmlReadWriteAdapter;
import com.nuodb.tools.migration.definition.xml.XmlReadContext;
import com.nuodb.tools.migration.definition.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import java.util.ArrayList;
import java.util.List;

public class MigrationHandler extends XmlReadWriteAdapter<Migration> implements XmlConstants {

    public MigrationHandler() {
        super(Migration.class);
    }

    @Override
    public boolean write(Migration migration, OutputNode output, XmlWriteContext context) throws Exception {
        output.getNamespaces().setReference(MIGRATION_NAMESPACE);
        for (Connection connection : migration.getConnections()) {
            context.write(connection, Connection.class, output.getChild(CONNECTION_ELEMENT));
        }
        for (Task task : migration.getTasks()) {
            context.write(task, Task.class, output.getChild(TASK_ELEMENT));
        }
        return true;
    }

    @Override
    protected void read(InputNode input, Migration migration, XmlReadContext context) throws Exception {
        List<Connection> connections = new ArrayList<Connection>();
        List<Task> tasks = new ArrayList<Task>();
        InputNode node;
        while ((node = input.getNext()) != null) {
            String name = node.getName();
            if (CONNECTION_ELEMENT.equals(name)) {
                connections.add(context.read(node, Connection.class));
            }
            if (TASK_ELEMENT.equals(name)) {
                tasks.add(context.read(node, Task.class));
            }
        }
        migration.setConnections(connections);
        migration.setTasks(tasks);
    }
}
