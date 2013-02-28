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
package com.nuodb.migrator.config.xml.handler;

import com.google.common.collect.Lists;
import com.nuodb.migrator.config.xml.XmlConstants;
import com.nuodb.migrator.config.xml.XmlReadContext;
import com.nuodb.migrator.config.xml.XmlWriteContext;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.MigrationSpec;
import com.nuodb.migrator.spec.TaskSpec;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import java.util.List;

public class XmlMigrationHandler extends XmlReadWriteHandlerBase<MigrationSpec> implements XmlConstants {

    public XmlMigrationHandler() {
        super(MigrationSpec.class);
    }

    @Override
    public boolean write(MigrationSpec migrationSpec, OutputNode output, XmlWriteContext context) throws Exception {
        output.getNamespaces().setReference(MIGRATION_NAMESPACE);
        for (ConnectionSpec connection : migrationSpec.getConnectionSpecs()) {
            context.write(connection, ConnectionSpec.class, output.getChild(CONNECTION_ELEMENT));
        }
        for (TaskSpec task : migrationSpec.getTaskSpecs()) {
            context.write(task, TaskSpec.class, output.getChild(TASK_ELEMENT));
        }
        return true;
    }

    @Override
    protected void read(InputNode input, MigrationSpec migrationSpec, XmlReadContext context) throws Exception {
        List<ConnectionSpec> connections = Lists.newArrayList();
        List<TaskSpec> tasks = Lists.newArrayList();
        InputNode node;

        while ((node = input.getNext()) != null) {
            String name = node.getName();
            if (CONNECTION_ELEMENT.equals(name)) {
                connections.add(context.read(node, ConnectionSpec.class));
            }
            if (TASK_ELEMENT.equals(name)) {
                tasks.add(context.read(node, TaskSpec.class));
            }
        }
        migrationSpec.setConnectionSpecs(connections);
        migrationSpec.setTaskSpecs(tasks);
    }
}
