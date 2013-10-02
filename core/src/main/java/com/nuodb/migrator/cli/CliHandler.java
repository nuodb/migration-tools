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
package com.nuodb.migrator.cli;

import com.nuodb.migrator.bootstrap.Bootable;
import com.nuodb.migrator.bootstrap.config.Config;
import com.nuodb.migrator.cli.parse.*;
import com.nuodb.migrator.cli.parse.help.HelpFormatter;
import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.cli.parse.parser.ParserImpl;
import com.nuodb.migrator.cli.processor.ConfigOptionProcessor;
import com.nuodb.migrator.cli.run.CliRun;
import com.nuodb.migrator.cli.run.CliRunJob;
import com.nuodb.migrator.cli.run.CliRunLookup;
import com.nuodb.migrator.job.JobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.context.ContextUtils.getMessage;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.lang.System.exit;
import static org.apache.commons.lang3.StringUtils.split;

/**
 * Main entry point of the command line interface, for the names of the available options see {@link CliOptions}.
 *
 * @author Sergey Bushik
 */
public class CliHandler extends CliSupport implements Bootable {

    public static final int CLI_ERROR = 2;

    protected transient final Logger logger = LoggerFactory.getLogger(getClass());

    public static final String EXECUTABLE = "bin/nuodb-migrator";

    private String executable = EXECUTABLE;
    private Parser parser = new ParserImpl();
    private CliRunLookup cliRunLookup = new CliRunLookup();

    @Override
    public void boot(Config config, String[] arguments) throws Exception {
        try {
            boot(config);
            Option option = createOption();
            OptionSet optionSet = getParser().parse(arguments, option);
            handleOptionSet(optionSet, option);
        } catch (OptionException exception) {
            handleOptionException(exception);
            exit(CLI_ERROR);
        }
    }

    @SuppressWarnings("UnusedParameters")
    protected void boot(Config config) {
        setExecutable(config.getProperty(Config.EXECUTABLE, EXECUTABLE));
    }

    public Parser getParser() {
        return parser;
    }

    protected Group createOption() {
        Option help = newBasicOptionBuilder().
                withId(HELP_OPTION_ID).
                withName(HELP_OPTION).
                withAlias(HELP_SHORT_OPTION, OptionFormat.SHORT).
                withDescription(getMessage(HELP_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(HELP_ARGUMENT_NAME)).build()
                ).build();

        Option list = newBasicOptionBuilder().
                withId(LIST_OPTION_ID).
                withName(LIST_OPTION).
                withAlias(LIST_SHORT_OPTION, OptionFormat.SHORT).
                withDescription(getMessage(LIST_OPTION_DESCRIPTION)).build();

        OptionFormat optionFormat = new OptionFormat(getOptionFormat());
        optionFormat.setValuesSeparator(null);

        Option config = newBasicOptionBuilder().
                withId(CONFIG_OPTION_ID).
                withName(CONFIG_OPTION).
                withAlias(CONFIG_SHORT_OPTION, OptionFormat.SHORT).
                withDescription(getMessage(CONFIG_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(CONFIG_ARGUMENT_NAME)).
                                withOptionFormat(optionFormat).
                                withOptionProcessor(new ConfigOptionProcessor()).
                                withMinimum(1).
                                withMaximum(MAX_VALUE).build()
                ).build();

        Command command = new CliRunCommand(cliRunLookup);
        command.setId(COMMAND_OPTION_ID);
        command.setName(COMMAND_OPTION);
        command.setDescription(getMessage(COMMAND_OPTION_DESCRIPTION));
        command.setHelpValues(newArrayList(split(getMessage(COMMAND_OPTION_HELP_VALUES))));
        command.setRequired(false);
        command.setOptionFormat(getOptionFormat());

        return newGroupBuilder().
                withName(getMessage(ROOT_GROUP_NAME)).
                withOption(help).
                withOption(list).
                withOption(config).
                withOption(command).
                withRequired(true).build();
    }

    protected void handleOptionSet(OptionSet options, Option root) {
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
        String command = (String) options.getValue(HELP_OPTION);
        HelpFormatter formatter = new HelpFormatter();
        if (command != null) {
            CliRun cliRun = cliRunLookup.lookup(command);
            if (cliRun != null) {
                formatter.setOption(cliRun);
                formatter.setExecutable(getExecutable() + " " + command);
            } else {
                throw new OptionException(format("Unknown command %s", command), options.getOption(HELP_OPTION));
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
        Collection<String> commands = cliRunLookup.getCommands();
        PrintStream writer = System.out;
        writer.println(getMessage(LIST_OPTION_OUTPUT));
        for (String command : commands) {
            writer.println(HelpFormatter.GUTTER + command);
        }
    }

    protected void handleConfig(OptionSet options) {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Handling --config %s option", options.getValue(CONFIG_OPTION)));
        }
    }

    protected void handleRun(OptionSet options) {
        CliRun cliRun = (CliRun) options.getValue(COMMAND_OPTION);
        if (logger.isTraceEnabled()) {
            logger.trace(format("Running %s command", cliRun.getCommand()));
        }
        if (cliRun instanceof CliRunJob) {
            configure(((CliRunJob) cliRun).getJobFactory());
        }
        cliRun.run();
    }

    protected void configure(JobFactory jobFactory) {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Configuring %s job factory", jobFactory.getClass()));
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
