package com.nuodb.tools.migration.definition.xml;

import com.nuodb.tools.migration.definition.DumpTask;
import com.nuodb.tools.migration.definition.JdbcConnection;
import com.nuodb.tools.migration.definition.Migration;
import com.nuodb.tools.migration.definition.xml.handlers.TypeableResolver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

public class XmlPersisterBuilder implements XmlConstants {

    public static final String HANDLERS_REGISTRY = "com/nuodb/tools/migration/definition/xml/handlers/handlers.registry";

    private XmlHandlerRegistryParser parser = new XmlHandlerRegistryParser();

    public XmlPersisterBuilder withHandlers(String resource) {
        parser.add(resource);
        return this;
    }

    public XmlPersister build() {
        XmlHandlerRegistry registry = new XmlHandlerRegistry();
        parser.parse(registry);

        TypeableResolver resolver = new TypeableResolver();
        resolver.bind(MIGRATION_NAMESPACE, CONNECTION_ELEMENT, JDBC, JdbcConnection.class);
        resolver.bind(MIGRATION_NAMESPACE, TASK_ELEMENT, DUMP, DumpTask.class);
        registry.register(resolver, XmlHandlerRegistry.PRIORITY_LOW);

        return new XmlPersister(registry);
    }

    public static void main(String[] args) throws IOException {
        XmlPersisterBuilder builder = new XmlPersisterBuilder();
        builder.withHandlers(HANDLERS_REGISTRY);
        XmlPersister persister = builder.build();

        Migration migration = new Migration();
        JdbcConnection connection = new JdbcConnection();
        connection.setId("mysql");
        connection.setDriver("com.mysql.jdbc.Driver");
        connection.setUrl("jdbc:mysql://localhost:3306/test");
        connection.setUsername("root");
        migration.setConnections(Arrays.asList(connection));


        System.out.println("Writing migration:");
        persister.write(migration, System.out);

        System.out.println("\n");
        System.out.println("Reading migration:");
        migration = persister.read(Migration.class,
                new ByteArrayInputStream((
                        "<?xml version=\"1.0\"?>\n" +
                        "<migration xmlns=\"http://nuodb.com/schema/migration\">\n" +
                        "   <task type=\"dump\"/>\n" +
                        "   <connection id=\"mysql\" type=\"jdbc\"/>\n" +
                        "   <connection id=\"\" type=\"jdbc\"/>\n" +
                        "</migration>").getBytes()));
        System.out.println(migration);
    }
}
