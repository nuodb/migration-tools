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

import com.nuodb.migration.cli.parse.*;
import com.nuodb.migration.utils.Priority;
import com.nuodb.migration.utils.PriorityList;
import com.nuodb.migration.utils.SimplePriorityList;

import java.util.*;

import static java.lang.String.format;

/**
 * A base implementation of option providing limited ground work for further implementations.
 */
public abstract class OptionBase implements Option {

    private int id;
    private String name;
    private String description;
    private boolean required;
    private OptionFormat optionFormat;
    private OptionProcessor optionProcessor;
    private PriorityList<Trigger> triggers = new SimplePriorityList<Trigger>();

    public OptionBase() {
    }

    public OptionBase(int id, String name, String description, boolean required) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.required = required;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    @Override
    public void setRequired(boolean required) {
        this.required = required;
    }

    @Override
    public OptionFormat getOptionFormat() {
        return optionFormat;
    }

    @Override
    public void setOptionFormat(OptionFormat optionFormat) {
        this.optionFormat = optionFormat;
    }

    @Override
    public OptionProcessor getOptionProcessor() {
        return optionProcessor;
    }

    @Override
    public void setOptionProcessor(OptionProcessor optionProcessor) {
        this.optionProcessor = optionProcessor;
    }

    @Override
    public void addTrigger(Trigger trigger) {
        addTrigger(trigger, Priority.NORMAL);
    }

    @Override
    public void addTrigger(Trigger trigger, int priority) {
        triggers.add(trigger, priority);
    }

    @Override
    public PriorityList<Trigger> getTriggers() {
        return triggers;
    }

    @Override
    public Option findOption(String trigger) {
        return findOption(new TriggerImpl(trigger));
    }

    @Override
    public Option findOption(Trigger trigger) {
        return getTriggers().contains(trigger) ? this : null;
    }

    public Set<String> getOptionPrefixes() {
        return optionFormat != null ? optionFormat.getPrefixes() : Collections.<String>emptySet();
    }

    public String getArgumentSeparator() {
        return optionFormat != null ? optionFormat.getArgumentSeparator() : null;
    }

    public String getArgumentValuesSeparator() {
        return optionFormat != null ? optionFormat.getValuesSeparator() : null;
    }

    @Override
    public boolean canProcess(CommandLine commandLine, ListIterator<String> arguments) {
        if (arguments.hasNext()) {
            try {
                String argument = arguments.next();
                return canProcess(commandLine, argument);
            } finally {
                arguments.previous();
            }
        } else {
            return false;
        }
    }

    @Override
    public void preProcess(CommandLine commandLine, ListIterator<String> arguments) {
        doPreProcess(commandLine, arguments);
        OptionProcessor optionProcessor = getOptionProcessor();
        if (optionProcessor != null) {
            optionProcessor.preProcess(commandLine, this, arguments);
        }
    }

    protected void doPreProcess(CommandLine commandLine, ListIterator<String> arguments) {
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        processArguments(commandLine, arguments);
        OptionProcessor optionProcessor = getOptionProcessor();
        if (optionProcessor != null) {
            optionProcessor.process(commandLine, this, arguments);
        }
    }

    protected void processArguments(CommandLine commandLine, ListIterator<String> arguments) {
    }

    @Override
    public void postProcess(CommandLine commandLine) {
        doPostProcess(commandLine);
        OptionProcessor optionProcessor = getOptionProcessor();
        if (optionProcessor != null) {
            optionProcessor.postProcess(commandLine, this);
        }
    }

    protected void doPostProcess(CommandLine commandLine) {
        if (isRequired() && !commandLine.hasOption(this)) {
            throw new OptionException(this, format("Missing required option %1$s", getName()));
        }
    }

    public static void createTriggers(PriorityList<Trigger> triggers, Set<String> prefixes, String trigger) {
        for (String prefix : prefixes) {
            triggers.add(new TriggerImpl(prefix + trigger));
        }
    }

    public static void join(StringBuilder help, Collection<?> values) {
        for (Iterator<?> iterator = values.iterator(); iterator.hasNext(); ) {
            help.append(iterator.next());
            if (iterator.hasNext()) {
                help.append(",");
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OptionBase option = (OptionBase) o;
        if (id != option.id) return false;
        if (name != null ? !name.equals(option.name) : option.name != null) return false;
        if (description != null ? !description.equals(option.description) : option.description != null) return false;
        if (required != option.required) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (required ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder help = new StringBuilder();
        help(help, HelpHint.ALL_HINTS, null);
        return help.toString();
    }
}
