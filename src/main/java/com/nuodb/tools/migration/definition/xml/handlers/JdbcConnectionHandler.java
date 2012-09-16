package com.nuodb.tools.migration.definition.xml.handlers;

import com.nuodb.tools.migration.definition.JdbcConnection;
import com.nuodb.tools.migration.definition.xml.XmlWriteContext;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Map;

public class JdbcConnectionHandler extends DefinitionHandler<JdbcConnection> {

    public JdbcConnectionHandler() {
        super(JdbcConnection.class);
    }

    @Override
    protected boolean write(JdbcConnection connection, OutputNode output, XmlWriteContext context) throws Exception {
        super.write(connection, output, context);
        output.getChild("catalog").setValue(connection.getCatalog());
        output.getChild("schema").setValue(connection.getSchema());
        output.getChild("driver").setValue(connection.getDriver());
        output.getChild("url").setValue(connection.getUrl());
        output.getChild("username").setValue(connection.getUsername());
        output.getChild("password").setValue(connection.getPassword());
        for (Map.Entry<String, String> entry : connection.getProperties().entrySet()) {
            OutputNode property = output.getChild("property");
            property.getChild("name").setValue(entry.getKey());
            property.getChild("value").setValue(entry.getValue());
        }
        return true;
    }
}
