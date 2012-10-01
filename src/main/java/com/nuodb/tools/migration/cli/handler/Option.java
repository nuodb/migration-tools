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
package com.nuodb.tools.migration.cli.handler;

import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 * The super type of all options representing a particular element of the command line interface.
 */
public interface Option {

    /**
     * Returns the id of the option.  This can be used in a loop and switch construct:
     * <p/>
     * <code> for(Option o : cmd.getOptions()){ switch(o.getId()){ case POTENTIAL_OPTION: ... } } </code>
     * <p/>
     * The returned value is not guaranteed to be unique.
     *
     * @return the id of the option.
     */
    int getId();

    /**
     * The name of an option is used for generating help and help information.
     *
     * @return The name of the option
     */
    String getName();

    /**
     * Returns a description of the option. This string is used to build help messages as in the help formatter.
     *
     * @return a description of the option.
     */
    String getDescription();

    /**
     * Indicates whether this option is required to be present.
     *
     * @return true if the command line will be invalid without this option
     */
    boolean isRequired();

    /**
     * Identifies the argument prefixes that should be considered options. This is used to identify whether a given
     * string looks like an option or an argument value. Typically an option would return the set [--,-] while switches
     * might offer [-,+].
     * <p/>
     * The returned Set must not be null.
     *
     * @return The set of prefixes for this Option
     */
    Set<String> getPrefixes();

    /**
     * Identifies the argument prefixes that should trigger this option. This is used to decide which of many Options
     * should be tried when processing a given argument string.
     * <p/>
     * The returned Set must not be null.
     *
     * @return The set of triggers for this Option
     */
    Set<String> getTriggers();

    /**
     * Recursively searches for an option with the supplied trigger.
     *
     * @param trigger the trigger to search for.
     * @return the matching option or null.
     */
    Option findOption(String trigger);

    /**
     * Adds defaults to a CommandLine.
     * <p/>
     * Any defaults for this option are applied as well as the defaults for any contained options
     *
     * @param commandLine command line object to store defaults in
     */
    void defaults(CommandLine commandLine);

    /**
     * Indicates whether this Option will be able to process the particular argument.
     *
     * @param commandLine command line to check
     * @param argument    The argument to be tested
     * @return true if the argument can be processed by this Option
     */
    boolean canProcess(CommandLine commandLine, String argument);

    /**
     * Indicates whether this Option will be able to process the particular argument. The list iterator must be restored
     * to the initial state before returning the boolean.
     *
     * @param commandLine the command line to check
     * @param arguments   the list iterator over String arguments
     * @return true if the argument can be processed by this Option
     * @see #canProcess(CommandLine, String)
     */
    boolean canProcess(CommandLine commandLine, ListIterator<String> arguments);

    /**
     * Processes String arguments into a command line.
     * <p/>
     * The iterator will initially point at the first argument to be processed and at the end of the method should point
     * to the first argument not processed. This method must process at least one argument from the list iterator.
     *
     * @param commandLine the command line object to store results in
     * @param arguments   the arguments to process.
     */
    void process(CommandLine commandLine, ListIterator<String> arguments);

    /**
     * Performs any required post processing, such as validation & value conversion.
     *
     * @param commandLine command line to check.
     * @throws OptionException if the command line is not valid.
     */
    void postProcess(CommandLine commandLine);

    /**
     * Appends help to the specified buffer
     *
     * @param buffer     the buffer to append to
     * @param hints      a set of output hints
     * @param comparator a comparator used to sort the options
     */
    void help(StringBuilder buffer, Set<HelpHint> hints, Comparator<Option> comparator);

    /**
     * Builds up a list of help lines instances to be presented by HelpFormatter.
     *
     * @param indent     the initial indent depth
     * @param hints      the help settings that should be applied
     * @param comparator a comparator used to sort options when applicable
     * @return a list of help lines objects
     */
    List<Help> help(int indent, Set<HelpHint> hints, Comparator<Option> comparator);
}
