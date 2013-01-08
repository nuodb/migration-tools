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
package com.nuodb.migration.cli.parse.option;

import com.google.common.collect.Iterables;
import com.nuodb.migration.cli.parse.*;

import java.util.*;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

/**
 * An implementation of an argument
 */
public class ArgumentImpl extends OptionBase implements Argument {

    public static final String NAME = "arg";

    private int minimum = 0;
    private int maximum = 1;
    private Collection<String> helpValues;
    private List<Object> defaultValues;
    private String valuesSeparator;

    public ArgumentImpl() {
    }

    public ArgumentImpl(int id, String name, String description, boolean required) {
        super(id, name != null ? name : NAME, description, required);
    }

    public ArgumentImpl(int id, String name, String description, boolean required,
                        int minimum, int maximum, List<Object> defaultValues, String valuesSeparator) {
        super(id, name != null ? name : NAME, description, required);
        this.minimum = minimum;
        this.maximum = maximum;
        this.defaultValues = defaultValues;
        this.valuesSeparator = valuesSeparator;
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
    public List<Object> getDefaultValues() {
        return defaultValues;
    }

    @Override
    public void setDefaultValues(List<Object> defaultValues) {
        this.defaultValues = defaultValues;
    }

    @Override
    public String getValuesSeparator() {
        return valuesSeparator;
    }

    @Override
    public void setValuesSeparator(String valuesSeparator) {
        this.valuesSeparator = valuesSeparator;
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
        processInternal(commandLine, arguments, option);
        OptionProcessor optionProcessor = getOptionProcessor();
        if (optionProcessor != null) {
            optionProcessor.process(commandLine, this, arguments);
        }
    }

    protected void processInternal(CommandLine commandLine, ListIterator<String> arguments, Option option) {
        int count = commandLine.getValues(option).size();
        while (arguments.hasNext() && (count < maximum)) {
            String value = arguments.next();
            if (commandLine.isOption(value)) {
                arguments.previous();
                break;
            }
            if (valuesSeparator != null) {
                StringTokenizer values = new StringTokenizer(value, valuesSeparator);
                arguments.remove();
                while (values.hasMoreTokens() && (count < maximum)) {
                    count++;
                    value = values.nextToken();
                    commandLine.addValue(option, value);
                    arguments.add(value);
                }
                if (values.hasMoreTokens()) {
                    throw new OptionException(this, format("Unexpected token %1$s", values.nextToken()));
                }
            } else {
                count++;
                commandLine.addValue(option, value);
            }
        }
    }

    @Override
    public void postProcess(CommandLine commandLine) {
        postProcess(commandLine, this);
    }

    @Override
    public void postProcess(CommandLine commandLine, Option option) throws OptionException {
        postProcessInternal(commandLine, option);
        OptionProcessor optionProcessor = getOptionProcessor();
        if (optionProcessor != null) {
            optionProcessor.postProcess(commandLine, this);
        }
    }

    @Override
    protected void doPostProcess(CommandLine commandLine) {
        postProcessInternal(commandLine, this);
    }

    protected void postProcessInternal(CommandLine commandLine, Option option) {
        List<Object> values = commandLine.getValues(option);
        int minimum = getMinimum();
        if (values.size() < minimum) {
            throw new OptionException(option, format("Missing value for %1$s argument", getName()));
        }
        int maximum = getMaximum();
        if (values.size() > maximum) {
            throw new OptionException(option, format("Too many values for %1$s argument", getName()));
        }
    }

    @Override
    public void help(StringBuilder buffer, Set<HelpHint> hints, Comparator<Option> comparator) {
        int minimum = getMinimum();
        int maximum = getMaximum();
        boolean optional = hints.contains(HelpHint.OPTIONAL);
        boolean numbered = (maximum > 1) && hints.contains(HelpHint.ARGUMENT_NUMBERED);
        boolean bracketed = hints.contains(HelpHint.ARGUMENT_BRACKETED);
        // if infinite args are allowed then crop the list
        int count;
        Collection<String> helpValues = getHelpValues();
        boolean useHelpValues;
        if (helpValues != null && helpValues.size() > 0) {
            count = helpValues.size();
            useHelpValues = true;
        } else {
            count = (maximum == Integer.MAX_VALUE) ? 2 : maximum;
            useHelpValues = false;
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
            buffer.append(useHelpValues ? Iterables.get(helpValues, i) : getName());
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
        if (!useHelpValues && maximum == Integer.MAX_VALUE) {
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
    public List<Help> help(int indent, Set<HelpHint> hints, Comparator<Option> comparator) {
        return singletonList((Help) new HelpImpl(this, indent));
    }
}
