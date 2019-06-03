/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nuodb.migrator.cli.parse.parser;

import com.google.common.collect.Lists;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.Parser;
import org.slf4j.Logger;

import java.util.List;
import java.util.ListIterator;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.join;
import static org.slf4j.LoggerFactory.getLogger;

public class ParserImpl implements Parser {

    protected transient final Logger logger = getLogger(getClass());

    /**
     * Parse the withConnection.arguments according to the specified options and
     * properties.
     *
     * @param arguments
     *            to parse.
     * @param option
     *            sets the option to parse against.
     * @return the option setValue object.
     */
    public OptionSet parse(String[] arguments, Option option) throws OptionException {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Parsing options %s", join(asList(arguments), " ")));
        }
        List<String> list = Lists.newArrayList(arguments);

        CommandLine commandLine = new CommandLineImpl(option, list);
        // pick up any defaults from the meta
        option.defaults(commandLine);
        // withConnection the options as far as possible
        ListIterator<String> iterator = list.listIterator();
        Object previous = null;
        while (option.canProcess(commandLine, iterator)) {
            // peek at the next item and backtrack
            String current = iterator.next();
            iterator.previous();
            // if we have just tried to process this instance
            if (current == previous) {
                // abort
                break;
            }
            previous = current;
            option.preProcess(commandLine, iterator);
            option.process(commandLine, iterator);
        }
        if (iterator.hasNext()) {
            throw new OptionException(format("Unexpected argument %s", iterator.next()), option);
        }
        option.postProcess(commandLine);
        return commandLine;
    }
}
