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
package com.nuodb.migrator.cli.parse.option;

import com.google.common.collect.Iterables;
import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Help;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.OptionProcessor;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.StringTokenizer;

import static com.nuodb.migrator.cli.parse.option.OptionUtils.*;
import static java.util.Collections.singletonList;
import static java.lang.Integer.MAX_VALUE;

/**
 * An implementation of an argument
 */
public class ArgumentImpl extends OptionBase implements Argument {

    public static final String NAME = "arg";

    private int minimum = 0;
    private int maximum = 1;
    private int minimumValue = 0;
    private int maximumValue = MAX_VALUE;

    private Collection<String> helpValues;
    private List<Object> defaultValues;

    public ArgumentImpl() {
    }

    public ArgumentImpl(int id, String name, String description, boolean required) {
        super(id, name != null ? name : NAME, description, required);
    }

    @Override
    public boolean isRequired() {
        return getMinimum() > 0;
    }

    @Override
    public int getMinimum() {
        return minimum;
    }

    @Override
    public void setMinimum(int minimum) {
        this.minimum = minimum;
    }

    @Override
    public int getMaximum() {
        return maximum;
    }

    @Override
    public void setMaximum(int maximum) {
        this.maximum = maximum;
    }

    @Override
    public int getMinimumValue() {
        return minimumValue;
    }

    @Override
    public void setMinimumValue(int minimum) {
        this.minimumValue = minimum;
    }

    @Override
    public int getMaximumValue() {
        return maximumValue;
    }

    @Override
    public void setMaximumValue(int maximum) {
        this.maximumValue = maximum;
    }

    @Override
    public List<Object> getDefaultValues() {
        return defaultValues;
    }

    @Override
    public void setDefaultValues(List<Object> defaultValues) {
        this.defaultValues = defaultValues;
    }

    @Override
    public Collection<String> getHelpValues() {
        return helpValues;
    }

    @Override
    public void setHelpValues(Collection<String> helpValues) {
        this.helpValues = helpValues;
    }

    @Override
    public Set<String> getPrefixes() {
        return Collections.emptySet();
    }

    @Override
    public void defaults(CommandLine commandLine) {
        defaults(commandLine, this);
    }

    @Override
    public void defaults(CommandLine commandLine, Option option) {
        commandLine.setDefaultValues(option, getDefaultValues());
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return true;
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        process(commandLine, arguments, this);
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments, Option option) {
        processArgument(commandLine, arguments, option);
        for (OptionProcessor optionProcessor : getOptionProcessors()) {
            optionProcessor.process(commandLine, this, arguments);
        }
    }

    protected void processArgument(CommandLine commandLine, ListIterator<String> arguments, Option option) {
        int count = commandLine.getValues(option).size();
        while (arguments.hasNext() && (count < maximum)) {
            String value = arguments.next();
            if (isOption(commandLine, value)) {
                arguments.previous();
                break;
            }
            String separator = getArgumentValuesSeparator();
            if (separator != null && value.length() > 0) {
                StringTokenizer values = new StringTokenizer(value, separator);
                arguments.remove();
                while (values.hasMoreTokens() && (count < maximumValue)) {
                    count++;
                    value = values.nextToken();
                    commandLine.addValue(option, value);
                    arguments.add(value);
                }
                if (values.hasMoreTokens()) {
                    postProcessUnexpected(values.nextToken());
                }
            } else {
                count++;
                commandLine.addValue(option, value.length() == 0 ? null : value);
            }
        }
    }

    protected boolean isCommand(CommandLine commandLine, String value) {
        return commandLine.isCommand(value);
    }

    protected boolean isOption(CommandLine commandLine, String value) {
        return commandLine.isOption(value);
    }

    protected void postProcessUnexpected(String argument) {
        optionUnexpected(this, argument);
    }

    @Override
    public void postProcess(CommandLine commandLine) {
        postProcess(commandLine, this);
    }

    @Override
    public void postProcess(CommandLine commandLine, Option option) throws OptionException {
        postProcessInternal(commandLine, option);
        for (OptionProcessor optionProcessor : getOptionProcessors()) {
            optionProcessor.postProcess(commandLine, this);
        }
    }

    @Override
    protected void postProcessOption(CommandLine commandLine) {
        postProcessInternal(commandLine, this);
    }

    protected void postProcessInternal(CommandLine commandLine, Option option) {
        List<Object> values = commandLine.getValues(option);
        int minimum = getMinimumValue();
        if (values.size() < minimum) {
            argumentMinimum(option, this);
        }
        int maximum = getMaximumValue();
        if (values.size() > maximum) {
            argumentMaximum(option, this);
        }
    }

    @Override
    public void help(StringBuilder buffer, Collection<HelpHint> hints, Comparator<Option> comparator) {
        int minimum = getMinimum();
        int maximum = getMaximum();
        boolean optional = hints.contains(HelpHint.OPTIONAL);
        boolean numbered = (maximum > 1) && hints.contains(HelpHint.ARGUMENT_NUMBERED);
        boolean bracketed = hints.contains(HelpHint.ARGUMENT_BRACKETED);
        // if infinite args are allowed then crop the list
        int count;
        Collection<String> helpValues = getHelpValues();
        boolean hasHelpValues;
        if (helpValues != null && helpValues.size() > 0) {
            count = helpValues.size();
            hasHelpValues = true;
        } else {
            count = (maximum == Integer.MAX_VALUE) ? 2 : maximum;
            hasHelpValues = false;
        }
        int i = 0;
        // for each argument
        while (i < count) {
            // if we're past the first append a space
            if (i > 0) {
                buffer.append(' ');
            }
            // if the next arg is optional
            if ((i >= minimum) && (optional || (i > 0))) {
                buffer.append('[');
            }
            if (bracketed) {
                buffer.append('<');
            }
            // append name
            buffer.append(hasHelpValues ? Iterables.get(helpValues, i) : getName());
            ++i;
            // if numbering
            if (numbered) {
                buffer.append(i);
            }
            if (bracketed) {
                buffer.append('>');
            }
        }
        // if infinite args are allowed
        if (!hasHelpValues && maximum == Integer.MAX_VALUE) {
            buffer.append(" ...");
        }
        // for each argument
        while (i > 0) {
            --i;
            // if the next arg is optional
            if ((i >= minimum) && (optional || (i > 0))) {
                buffer.append(']');
            }
        }
    }

    @Override
    public List<Help> help(int indent, Collection<HelpHint> hints, Comparator<Option> comparator) {
        return singletonList((Help) new HelpImpl(this, indent));
    }
}
