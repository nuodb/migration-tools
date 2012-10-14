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

    private int minimum = 0;
    private int maximum = 1;
    private List<Object> defaultValues;
    private String valuesSeparator;

    public ArgumentImpl() {
    }

    public ArgumentImpl(int id, String name, String description, boolean required) {
        super(id, name, description, required);
    }

    public ArgumentImpl(int id, String name, String description, boolean required,
                        int minimum, int maximum, List<Object> defaultValues, String valuesSeparator) {
        super(id, name, description, required);
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
        int count = commandLine.getOptionValues(option).size();
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
                    throw new OptionException(this, String.format("Unexpected token %1$s", values.nextToken()));
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
        doBind(commandLine, option);
        doValidate(commandLine, option);
    }

    @Override
    protected void doBind(CommandLine commandLine) {
        doBind(commandLine, this);
    }

    protected void doBind(CommandLine commandLine, Option option) {
    }

    @Override
    protected void doValidate(CommandLine commandLine) {
        doValidate(commandLine, this);
    }

    protected void doValidate(CommandLine commandLine, Option option) {
        List values = commandLine.getValues(option);
        int minimum = getMinimum();
        if (values.size() < minimum) {
            throw new OptionException(option, String.format("Missing value for %1$s argument", getName()));
        }
        int maximum = getMaximum();
        if (values.size() > maximum) {
            throw new OptionException(option, String.format("Too many values for %1$s argument", getName()));
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
