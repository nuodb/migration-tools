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
package com.nuodb.migration.cli;

import com.nuodb.migration.cli.parse.*;
import com.nuodb.migration.cli.parse.help.HelpFormatter;
import com.nuodb.migration.cli.parse.option.OptionToolkit;
import com.nuodb.migration.cli.parse.parser.ParserImpl;
import com.nuodb.migration.cli.run.CliRun;
import com.nuodb.migration.cli.run.CliRunFactory;
import com.nuodb.migration.cli.run.CliRunFactoryLookup;
import com.nuodb.migration.cli.run.CliRunJob;
import com.nuodb.migration.context.support.ApplicationSupport;
import com.nuodb.migration.jdbc.connection.ConnectionProviderFactory;
import com.nuodb.migration.job.JobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Collection;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class CliHandlerSupport extends ApplicationSupport implements CliResources, CliOptions {

    protected transient final Logger logger = LoggerFactory.getLogger(getClass());

    public static final String EXECUTABLE = "bin/nuodb-migration";

    private String executable = EXECUTABLE;
    private OptionToolkit optionToolkit;
    private CliRunFactoryLookup cliRunFactoryLookup;
    private Parser parser = new ParserImpl();

    protected CliHandlerSupport() {
        this(new OptionToolkit());
    }

    protected CliHandlerSupport(OptionToolkit optionToolkit) {
        this(optionToolkit, new CliRunFactoryLookup(optionToolkit));
    }

    protected CliHandlerSupport(OptionToolkit optionToolkit, CliRunFactoryLookup cliRunFactoryLookup) {
        this.optionToolkit = optionToolkit;
        this.cliRunFactoryLookup = cliRunFactoryLookup;
    }

    public Parser getParser() {
        return parser;
    }

    protected Group createOption() {
        Option help = optionToolkit.newOption().
                withId(HELP_OPTION_ID).
                withName(HELP_OPTION).
                withDescription(getMessage(HELP_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(getMessage(HELP_ARGUMENT_NAME)).build()
                ).build();
        Option list = optionToolkit.newOption().
                withId(LIST_OPTION_ID).
                withName(LIST_OPTION).
                withDescription(getMessage(LIST_OPTION_DESCRIPTION)).build();
        Option config = optionToolkit.newOption().
                withId(CONFIG_OPTION_ID).
                withName(CONFIG_OPTION).
                withDescription(getMessage(CONFIG_OPTION_DESCRIPTION)).
                withArgument(
                        optionToolkit.newArgument().
                                withName(getMessage(CONFIG_ARGUMENT_NAME)).
                                withMinimum(1).
                                withMaximum(1).build()
                ).build();

        Option command = new CliCommand(COMMAND_OPTION_ID, COMMAND_OPTION, getMessage(COMMAND_OPTION_DESCRIPTION),
                false, cliRunFactoryLookup);
        return optionToolkit.newGroup().
                withName(getMessage(ROOT_GROUP_NAME)).
                withOption(help).
                withOption(list).
                withOption(config).
                withOption(command).
                withRequired(true).build();
    }

    protected void handleOptionSet(OptionSet options, Option root) {
        if (logger.isTraceEnabled()) {
            logger.trace("Options successfully parsed");
        }
        if (options.getOptions().isEmpty()) {
            handleHelp(options, root);
        } else if (options.hasOption(HELP_OPTION)) {
            if (logger.isTraceEnabled()) {
                logger.trace("Handling --help option");
            }
            handleHelp(options, root);
        } else if (options.hasOption(COMMAND_OPTION)) {
            handleRun(options);
        } else if (options.hasOption(CONFIG_OPTION)) {
            handleConfig(options);
        } else if (options.hasOption(LIST_OPTION)) {
            handleList();
        }
    }

    protected void handleHelp(OptionSet options, Option root) {
        String command = options.getValue(HELP_OPTION);
        HelpFormatter formatter = new HelpFormatter();
        if (command != null) {
            CliRunFactory cliRunFactory = cliRunFactoryLookup.lookup(command);
            if (cliRunFactory != null) {
                formatter.setOption(cliRunFactory.createCliRun());
                formatter.setExecutable(getExecutable() + " " + command);
            } else {
                throw new OptionException(options.getOption(HELP_OPTION), format("Unknown command %s", command));
            }
        } else {
            formatter.setOption(root);
            formatter.setExecutable(getExecutable());
        }
        formatter.format(System.out);
    }

    protected void handleList() {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Handling --list option"));
        }
        Collection<String> commands = cliRunFactoryLookup.getCommands();
        PrintStream writer = System.out;
        writer.println(getMessage(LIST_OPTION_OUTPUT));
        for (String command : commands) {
            writer.println(HelpFormatter.GUTTER + command);
        }
    }

    protected void handleConfig(OptionSet options) {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Handling --config %1$s option", options.getValue("config")));
        }
    }

    protected void handleRun(OptionSet options) {
        CliRun cliRun = options.getValue(COMMAND_OPTION);
        if (logger.isTraceEnabled()) {
            logger.trace(format("Running %1$s command", cliRun.getCommand()));
        }
        if (cliRun instanceof CliRunJob) {
            configure(((CliRunJob) cliRun).getJobFactory());
        }
        cliRun.run();
    }

    protected void configure(JobFactory jobFactory) {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Configuring %1$s job factory", jobFactory.getClass()));
        }
        if (jobFactory instanceof ConnectionProviderFactory) {
            ConnectionProviderFactory connectionProviderFactory = (ConnectionProviderFactory) jobFactory;
            connectionProviderFactory.setLogging(true);
            connectionProviderFactory.setPooling(true);
        }
    }

    protected void handleOptionException(OptionException exception) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setException(exception);
        Option option = exception.getOption();
        String executable;
        if (option instanceof CliRun) {
            String command = ((CliRun) option).getCommand();
            executable = format(getExecutable() + " " + command);
        } else {
            executable = getExecutable();
        }
        formatter.setExecutable(executable);
        formatter.format(System.out);
    }

    public String getExecutable() {
        return executable;
    }

    public void setExecutable(String executable) {
        this.executable = executable;
    }
}
