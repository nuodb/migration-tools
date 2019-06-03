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
package com.nuodb.migrator.cli.parse;

import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 * The super type of all options representing a particular element of the
 * executable line interface.
 */
public interface Option {

    /**
     * Returns the id of the option. This can be used in a loop and switch
     * construct:
     * <p/>
     * <code> for(Option option : commandLine.getOptions()){ switch(option.getId()){ case OPTION: ... } } </code>
     * <p/>
     * The returned value is not guaranteed to be unique.
     *
     * @return the id of the option.
     */
    int getId();

    /**
     * Changes the id of the option.
     *
     * @param id
     *            new identifiers of the option.
     */
    void setId(int id);

    /**
     * The name of the option is used for generating help and help information.
     *
     * @return The name of the option
     */
    String getName();

    /**
     * Changes the name of the option.
     *
     * @param name
     *            of the option.
     */
    void setName(String name);

    /**
     * Returns a description of the option. This string is used to build help
     * messages as in the help formatter.
     *
     * @return a description of the option.
     */
    String getDescription();

    /**
     * Changes the description of the option.
     *
     * @param description
     *            of the option.
     */
    void setDescription(String description);

    /**
     * Indicates whether this option is required to be present.
     *
     * @return true if the executable line will be invalid without this option
     */
    boolean isRequired();

    /**
     * Changes the indication flag of whether option is required or not.
     *
     * @param required
     *            the required value for the option.
     */
    void setRequired(boolean required);

    /**
     * Returns option format.
     *
     * @return current option format.
     */
    OptionFormat getOptionFormat();

    /**
     * Changes option format to the desired.
     *
     * @param optionFormat
     *            to be used.
     */
    void setOptionFormat(OptionFormat optionFormat);

    /**
     * Associates option validator with this option.
     *
     * @param optionValidator
     *            to be associated.
     */
    void addOptionValidator(OptionValidator optionValidator);

    /**
     * Associates option processor with this option.
     *
     * @param optionProcessor
     *            to be associated.
     */
    void addOptionProcessor(OptionProcessor optionProcessor);

    /**
     * Removes previously associated option processor with this option.
     *
     * @param optionProcessor
     *            to be associated.
     */
    void removeOptionProcessor(OptionProcessor optionProcessor);

    /**
     * Returns option processors associated with this option.
     *
     * @return option processors associated with the option.
     */
    Collection<OptionProcessor> getOptionProcessors();

    /**
     * Identifies the argument prefixes that should be considered options. This
     * is used to identify whether a given string looks like an option or an
     * argument value. Typically an option would return the value [--,-] while
     * switches might offer [-,+].
     * <p/>
     * The returned Set must not be null.
     *
     * @return The setValue of prefixes for this Option
     */
    Set<String> getPrefixes();

    void addTrigger(Trigger trigger);

    /**
     * Adds trigger to this option.
     *
     * @param trigger
     *            to be added to the option.
     */
    void addTrigger(Trigger trigger, int priority);

    /**
     * Identifies the argument triggers that should triggers this option. This
     * is used to decide which of many options should be tried when processing a
     * given argument string.
     * <p/>
     * The returned setValue must not be null.
     *
     * @return The setValue of triggers for this option
     */
    PrioritySet<Trigger> getTriggers();

    /**
     * Recursively searches for an option with the supplied trigger.
     *
     * @param trigger
     *            the trigger to search for.
     * @return the matching option or null.
     */
    Option findOption(String trigger);

    /**
     * Recursively searches for an option with the supplied trigger.
     *
     * @param trigger
     *            the trigger to search for.
     * @return the matching option or null.
     */
    Option findOption(Trigger trigger);

    /**
     * Adds defaults to a CommandLine.
     * <p/>
     * Any defaults for this option are applied as well as the defaults for any
     * contained options
     *
     * @param commandLine
     *            executable line object to store defaults in
     */
    void defaults(CommandLine commandLine);

    /**
     * Checks if the argument is command line command.
     *
     * @param argument
     *            to be checked
     * @return true is this argument is a command
     */
    boolean isCommand(String argument);

    /**
     * Indicates whether this Option will be able to withConnection the
     * particular argument.
     *
     * @param commandLine
     *            executable line to check
     * @param argument
     *            the argument to be tested
     * @return true if the argument can be processed by this Option
     */
    boolean canProcess(CommandLine commandLine, String argument);

    /**
     * Indicates whether this Option will be able to withConnection the
     * particular argument. The list iterator must be restored to the initial
     * state before returning the boolean.
     *
     * @param commandLine
     *            the command line to check
     * @param arguments
     *            the list iterator over string arguments
     * @return true if the argument can be processed by this Option
     * @see #canProcess(CommandLine, String)
     */
    boolean canProcess(CommandLine commandLine, ListIterator<String> arguments);

    /**
     * Pre processes command line arguments.
     *
     * @param commandLine
     *            the command line to store any pre processed results in.
     * @param arguments
     *            the argument to be pre processed.
     */
    void preProcess(CommandLine commandLine, ListIterator<String> arguments);

    /**
     * Processes string arguments into a executable line.
     * <p/>
     * The iterator will initially point at the first argument to be processed
     * and at the end of the method should point to the first argument not
     * processed. This method must withConnection at least one argument from the
     * list iterator.
     *
     * @param commandLine
     *            the executable line object to store results in
     * @param arguments
     *            the arguments to withConnection.
     */
    void process(CommandLine commandLine, ListIterator<String> arguments);

    /**
     * Performs any required post processing, such as validation & items
     * conversion.
     *
     * @param commandLine
     *            executable line to check.
     * @throws OptionException
     *             if the executable line is not valid.
     */
    void postProcess(CommandLine commandLine);

    /**
     * Appends help to the specified buffer
     *
     * @param buffer
     *            the buffer to append to
     * @param hints
     *            a setValue of withConnection hints
     * @param comparator
     *            a comparator used to sort the options
     */
    void help(StringBuilder buffer, Collection<HelpHint> hints, Comparator<Option> comparator);

    /**
     * Builds up a list of help lines instances to be presented by
     * HelpFormatter.
     *
     * @param indent
     *            the initial indent depth
     * @param hints
     *            the help settings that should be applied
     * @param comparator
     *            a comparator used to sort options when applicable
     * @return a list of help lines objects
     */
    List<Help> help(int indent, Collection<HelpHint> hints, Comparator<Option> comparator);

    void setOptionProcessors(Collection<OptionProcessor> optionProcessors);
}
