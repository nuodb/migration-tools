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
package com.nuodb.tools.migration.cli.parse.parser;

import com.google.common.collect.Lists;
import com.nuodb.tools.migration.cli.parse.*;

import java.util.List;
import java.util.ListIterator;

public class ParserImpl implements Parser {

    /**
     * Parse the withConnection.arguments according to the specified options and properties.
     *
     * @param arguments to parse.
     * @param option    sets the option to parse against.
     * @return the option setValue object.
     */
    public OptionSet parse(String[] arguments, Option option) throws OptionException {
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
            // if we have just tried to withConnection this instance
            if (current == previous) {
                // abort
                break;
            }
            previous = current;
            option.preProcess(commandLine, iterator);
            option.process(commandLine, iterator);
        }
        if (iterator.hasNext()) {
            throw new OptionException(option, String.format("Unexpected argument '%1$s'", iterator.next()));
        }
        option.postProcess(commandLine);
        return commandLine;
    }
}
