/**
 * Copyright (c) 2015, NuoDB, Inc.
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
package com.nuodb.migrator.cli.processor;

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.utils.ConfigUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.ListIterator;

import static com.nuodb.migrator.cli.CliOptions.CONFIG;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * Expands --config (-c)=<file> [<file> ...] into a list of options loaded from
 * the path and removes expanded path from the iterator of arguments
 *
 * @author Sergey Bushik
 */
public class ConfigOptionProcessor extends ValuesOptionProcessor {

    @Override
    public void preProcess(CommandLine commandLine, Option option, ListIterator<String> arguments) {
    }

    @Override
    public void process(CommandLine commandLine, Option option, ListIterator<String> arguments) {
        for (String path : getValues(commandLine, CONFIG)) {
            addConfig(option, arguments, path);
        }
    }

    protected void addConfig(Option option, ListIterator<String> arguments, String path) {
        int delta = 0;
        for (String argument : parseConfig(option, path)) {
            arguments.add(argument);
            delta++;
        }
        for (int i = delta; i > 0; i--) {
            arguments.previous();
        }
    }

    /**
     * Loads list of config parameters from the specified path, ignores line
     * starting with comment symbol #
     *
     * @param option
     *            which defines target config option
     * @param path
     *            path to the config, which can be a class path resources, a url
     *            or a file
     * @return list of loaded parameters, commented lines are ignored
     * @throws OptionException
     *             if the config can't be loaded
     */
    protected Collection<String> parseConfig(Option option, String path) {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Loading config from %s", path));
        }
        InputStream input = ConfigUtils.loadConfig(path);
        if (input == null) {
            throw new OptionException(format("Can't load config from %s", path), option);
        }
        try {
            Collection<String> config = ConfigUtils.parseConfig(input);
            if (logger.isTraceEnabled()) {
                logger.trace(format("Loaded config is %s", join(config, " ")));
            }
            return config;
        } catch (IOException exception) {
            throw new OptionException(format("Can't load config from %s", path), exception, option);
        }
    }

    @Override
    public void postProcess(CommandLine commandLine, Option option) {
    }
}
