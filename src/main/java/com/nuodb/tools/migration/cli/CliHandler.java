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
import com.nuodb.tools.migration.cli.handler.OptionException;
import com.nuodb.tools.migration.cli.handler.OptionSet;
import com.nuodb.tools.migration.cli.handler.help.HelpFormatter;
import com.nuodb.tools.migration.cli.handler.parser.ParserImpl;
import com.nuodb.tools.migration.cli.handler.toolkit.OptionToolkit;

/**
 * @author Sergey Bushik
 */
public class CliHandler {

    private OptionToolkit toolkit;

    public CliHandler() {
        this(new OptionToolkit());
    }

    public CliHandler(OptionToolkit toolkit) {
        this.toolkit = toolkit;
    }

    public void handle(String[] arguments) throws OptionException {
        Group migration = new CliMigrationBuilder(toolkit).build();
        OptionSet line = new ParserImpl().parse(arguments, migration);
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setOption(migration);
            formatter.setCommand("migration");
            formatter.format(System.out);
        } else if (line.hasOption("task")) {
            System.out.println(String.format("Task %1$s", line.getValue("task")));
        } else if (line.hasOption("config")) {
            System.out.println(String.format("Configuration %1$s", line.getValue("config")));
        }
    }

    public static void main(String[] args) throws OptionException {
        CliHandler handler = new CliHandler();
        // handler.handle(new String[]{"--task=dump"});
        handler.handle(new String[]{"---help"});
    }
}
