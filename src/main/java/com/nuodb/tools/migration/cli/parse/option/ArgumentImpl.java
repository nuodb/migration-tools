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
package com.nuodb.tools.migration.cli.parse.option;

import com.nuodb.tools.migration.cli.parse.*;

import java.util.*;

/**
 * An implementation of an argument
 */
public class ArgumentImpl extends BaseOption implements Argument {

    public static final String NAME = "arg";

    private int minimum;
    private int maximum;
    private List<Object> defaultValues;
    private String valuesSeparator;

    public ArgumentImpl(int id, String name, String description, boolean required) {
        super(id, name, description, required);
        this.minimum = 0;
        this.maximum = 1;
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
    public Set<Trigger> getTriggers() {
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
                    throw new OptionException(this, String.format("Unexpected token %1$s", values.nextToken()));
                }
            } else {
                valuesCount++;
                line.addValue(option, value);
            }
        }
    }

    @Override
    public void validate(CommandLine commandLine) throws OptionException {
        validate(commandLine, this);
    }

    @Override
    public void validate(CommandLine commandLine, Option option) throws OptionException {
        List values = commandLine.getValues(option);
        int minimum = getMinimum();
        if (values.size() < minimum) {
            throw new OptionException(option, String.format("Missing value for %1$s argument", getName()));
        }
        int maximum = getMaximum();
        if (values.size() > maximum) {
            throw new OptionException(option, String.format("Too many arguments for %1$s", getName()));
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
        return Collections.singletonList((Help) new HelpImpl(this, indent));
    }
}
