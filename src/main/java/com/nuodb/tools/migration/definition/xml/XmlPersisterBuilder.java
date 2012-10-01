/**
 * Copyright (c) 2012, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.tools.migration.definition.xml;

import com.nuodb.tools.migration.definition.Definition;
import com.nuodb.tools.migration.definition.DumpTask;
import com.nuodb.tools.migration.definition.JdbcConnection;
import com.nuodb.tools.migration.definition.Migration;
import com.nuodb.tools.migration.definition.xml.handler.XmlTypeAliasMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

public class XmlPersisterBuilder implements XmlConstants {

    public static final String HANDLERS_REGISTRY = "com/nuodb/tools/migration/definition/xml/xml.handler.registry";

    private XmlHandlerRegistryParser parser = new XmlHandlerRegistryParser();

    public XmlPersisterBuilder addRegistry(String registry) {
        parser.addRegistry(registry);
        return this;
    }

    public XmlPersister build() {
        XmlHandlerRegistry registry = new XmlHandlerRegistry();
        parser.parse(registry);

        XmlTypeAliasMapper<Definition> handler = new XmlTypeAliasMapper<Definition>();
        handler.bind(MIGRATION_NAMESPACE, CONNECTION_ELEMENT, JDBC, JdbcConnection.class);
        handler.bind(MIGRATION_NAMESPACE, TASK_ELEMENT, DUMP, DumpTask.class);
        registry.register(handler, XmlHandlerRegistry.PRIORITY_LOW);

        return new XmlPersister(registry);
    }

    public static void main(String[] args) throws IOException {
        XmlPersisterBuilder builder = new XmlPersisterBuilder();
        builder.addRegistry(HANDLERS_REGISTRY);
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
