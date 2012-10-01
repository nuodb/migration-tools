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
package com.nuodb.tools.migration.cli.handler.option;

import com.nuodb.tools.migration.cli.handler.Argument;
import com.nuodb.tools.migration.cli.handler.CommandLine;
import com.nuodb.tools.migration.cli.handler.Help;
import com.nuodb.tools.migration.cli.handler.HelpHint;
import com.nuodb.tools.migration.cli.handler.Option;
import com.nuodb.tools.migration.cli.handler.OptionException;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * An implementation of an argument
 */
public class SimpleArgument extends BaseOption implements Argument {

    public static final String NAME = "arg";

    private int minimum;
    private int maximum;
    private List<Object> defaultValues;
    private String valuesSeparator;

    public SimpleArgument(int id, String name, String description,
                          int minimum, int maximum, List<Object> defaultValues, String valuesSeparator) {
        super(id, name != null ? name : NAME, description);
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
    public int getMaximum() {
        return maximum;
    }

    @Override
    public List<Object> getDefaultValues() {
        return defaultValues;
    }

    @Override
    public Set<String> getPrefixes() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getTriggers() {
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
    public Option findOption(String trigger) {
        return null;
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return true;
    }

    @Override
    public void process(CommandLine line, ListIterator<String> arguments) {
        process(line, arguments, this);
    }

    @Override
    public void process(CommandLine line, ListIterator<String> arguments, Option option) {
        int valuesCount = line.getOptionValues(option).size();
        while (arguments.hasNext() && (valuesCount < maximum)) {
            String value = arguments.next();
            if (line.isOption(value)) {
                arguments.previous();
                break;
            }
            if (valuesSeparator != null) {
                StringTokenizer values = new StringTokenizer(value, valuesSeparator);
                arguments.remove();
                while (values.hasMoreTokens() && (valuesCount < maximum)) {
                    valuesCount++;
                    value = values.nextToken();
                    line.addValue(option, value);
                    arguments.add(value);
                }
                if (values.hasMoreTokens()) {
                    throw new OptionException(this, "Unexpected value " + values.nextToken());
                }
            } else {
                valuesCount++;
                line.addValue(option, value);
            }
        }
    }

    @Override
    public void postProcess(CommandLine commandLine) throws OptionException {
        postProcess(commandLine, this);
    }

    @Override
    public void postProcess(CommandLine commandLine, Option option) throws OptionException {
        List values = commandLine.getValues(option);
        int minimum = getMinimum();
        if (values.size() < minimum) {
            throw new OptionException(option, String.format("Minimum number of %d arguments is expected", minimum));
        }
        int maximum = getMaximum();
        if (values.size() > maximum) {
            throw new OptionException(option, String.format("Maximum number of %d arguments is expected", maximum));
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
        int max = (maximum == Integer.MAX_VALUE) ? 2 : maximum;
        int i = 0;

        // for each argument
        while (i < max) {
            // if we're past the first add a space
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
            // add name
            buffer.append(getName());
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
        if (maximum == Integer.MAX_VALUE) {
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
        return Collections.singletonList((Help) new SimpleHelp(this, indent));
    }
}
