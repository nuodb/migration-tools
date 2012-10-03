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

import com.nuodb.tools.migration.cli.command.CliRunnable;
import com.nuodb.tools.migration.cli.command.CliRunnableFactoryLookup;
import com.nuodb.tools.migration.cli.handler.Group;
import com.nuodb.tools.migration.cli.handler.Option;
import com.nuodb.tools.migration.cli.handler.OptionException;
import com.nuodb.tools.migration.cli.handler.OptionSet;
import com.nuodb.tools.migration.cli.handler.help.HelpFormatter;
import com.nuodb.tools.migration.cli.handler.option.OptionToolkit;
import com.nuodb.tools.migration.cli.handler.parser.ParserImpl;
import com.nuodb.tools.migration.i18n.Resources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Bushik
 */
public class CliMigrationHandler implements CliResources {

    private static final int HELP_OPTION_ID = 1;
    private static final int LIST_OPTION_ID = 2;
    private static final int CONFIG_OPTION_ID = 3;
    private static final int COMMAND_OPTION_ID = 4;

    private static final String HELP_OPTION = "help";
    private static final String LIST_OPTION = "list";
    private static final String CONFIG_OPTION = "config";
    private static final String COMMAND_OPTION = "command";

    private static final String MIGRATION_EXECUTABLE = "migration";

    public void handle(String[] arguments) throws OptionException {
        Option root = createOption();
        try {
            OptionSet options = new ParserImpl().parse(arguments, root);
            handleOptionSet(options, root);
        } catch (OptionException exception) {
            handleOptionException(exception);
        }
    }

    protected Group createOption() {
        OptionToolkit optionToolkit = new OptionToolkit();
        Resources resources = Resources.getResources();
        Option help = optionToolkit.newOption().
                withId(HELP_OPTION_ID).
                withName(HELP_OPTION).
                withDescription(resources.getMessage(HELP_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(HELP_ARGUMENT_NAME)).build()
                ).build();
        Option list = optionToolkit.newOption().
                withId(LIST_OPTION_ID).
                withName(LIST_OPTION).
                withDescription(resources.getMessage(LIST_OPTION_DESCRIPTION)).build();
        Option config = optionToolkit.newOption().
                withId(CONFIG_OPTION_ID).
                withName(CONFIG_OPTION).
                withDescription(resources.getMessage(CONFIG_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(resources.getMessage(CONFIG_ARGUMENT_NAME)).
                                withMinimum(1).
                                withMaximum(1).build()
                ).build();
        Option command = new CliCommand(
                COMMAND_OPTION_ID, COMMAND_OPTION, resources.getMessage(COMMAND_OPTION_DESCRIPTION), false,
                new CliRunnableFactoryLookup(), optionToolkit);
        return optionToolkit.newGroup().
                withName(resources.getMessage(MIGRATION_GROUP_NAME)).
                withOption(help).
                withOption(list).
                withOption(config).
                withOption(command).
                withRequired(true).build();
    }

    protected void handleOptionSet(OptionSet options, Option root) {
        if (options.hasOption(HELP_OPTION)) {
            System.out.println("Handling --help");
            HelpFormatter formatter = new HelpFormatter();
            formatter.setOption(root);
            formatter.setExecutable(MIGRATION_EXECUTABLE);
            formatter.format(System.out);
        } else if (options.hasOption(COMMAND_OPTION)) {
            CliRunnable runnable = options.getValue(COMMAND_OPTION);
            System.out.println(String.format("Handling --command %1$s", runnable.getCommand()));
            runnable.run();
        } else if (options.hasOption(CONFIG_OPTION)) {
            System.out.println(String.format("Handling --config %1$s", options.getValue("config")));
        }
    }

    protected void handleOptionException(OptionException exception) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setException(exception);
        Option option = exception.getOption();
        String executable;
        if (option instanceof CliRunnable) {
            String command = ((CliRunnable) option).getCommand();
            executable = MIGRATION_EXECUTABLE + " " + command;
        } else {
            executable = MIGRATION_EXECUTABLE;
        }
        formatter.setExecutable(executable);
        formatter.format(System.out);
    }

    public static void main(String[] args) throws IOException {
        CliMigrationHandler handler = new CliMigrationHandler();
        handler.handle(loadArguments("arguments.properties"));
    }

    private static String[] loadArguments(String resource) throws IOException {
        InputStream input = CliMigrationHandler.class.getResourceAsStream(resource);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        List<String> arguments = new ArrayList<String>();
        String line;
        while ((line = reader.readLine()) != null) {
            arguments.add(line.trim());
        }
        return arguments.toArray(new String[arguments.size()]);
    }
}
