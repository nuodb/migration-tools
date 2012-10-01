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
package com.nuodb.tools.migration.cli;

import com.nuodb.tools.migration.cli.handler.Group;
import com.nuodb.tools.migration.cli.handler.Option;
import com.nuodb.tools.migration.cli.handler.toolkit.OptionToolkit;

/**
 * @author Sergey Bushik
 */
public class CliMigrationBuilder {

    private OptionToolkit toolkit;

    public CliMigrationBuilder(OptionToolkit toolkit) {
        this.toolkit = toolkit;
    }

    public Group build() {
        Option task = toolkit.newArgument().
                withName("task").
                withDescription("Executes specified type of migration task").build();
        Option help = toolkit.newOption().
                withName("help").
                withDescription("Prints help contents").
                withArgument(
                        toolkit.newArgument().
                                withName("command").build()
                ).build();
        Option list = toolkit.newOption().
                withName("list").
                withDescription("Lists available types of migrations").build();
        Option config = toolkit.newOption().
                withName("config").
                withDescription("Reads definition of the migration from the config file and executes it").
                withArgument(
                        toolkit.newArgument().
                                withName("file").
                                withMinimum(1).
                                withMaximum(1).build()
                ).build();
        return toolkit.newGroup().
                withName("migration").
                withOption(task).
                withOption(help).
                withOption(list).
                withOption(config).build();
    }
}
