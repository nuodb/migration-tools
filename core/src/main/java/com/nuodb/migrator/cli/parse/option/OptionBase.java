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

import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionProcessor;
import com.nuodb.migrator.cli.parse.OptionValidator;
import com.nuodb.migrator.cli.parse.Trigger;
import com.nuodb.migrator.utils.Priority;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.nuodb.migrator.cli.parse.OptionValidators.toOptionProcessor;
import static com.nuodb.migrator.cli.parse.option.OptionUtils.optionRequired;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;

/**
 * A base implementation of the option providing limited ground work for further
 * implementations.
 */
public abstract class OptionBase implements Option {

    private int id;
    private String name;
    private String description;
    private boolean required;
    private OptionFormat optionFormat = OptionFormat.LONG;
    private PrioritySet<Trigger> triggers = com.nuodb.migrator.utils.Collections.newPrioritySet();
    private Collection<OptionProcessor> optionProcessors = newLinkedHashSet();

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
    public void addOptionValidator(OptionValidator optionValidator) {
        addOptionProcessor(toOptionProcessor(optionValidator));
    }

    @Override
    public void addOptionProcessor(OptionProcessor optionProcessor) {
        isNotNull(optionProcessor);
        optionProcessors.add(optionProcessor);
    }

    @Override
    public void removeOptionProcessor(OptionProcessor optionProcessor) {
        isNotNull(optionProcessor);
        optionProcessors.remove(optionProcessor);
    }

    @Override
    public Collection<OptionProcessor> getOptionProcessors() {
        return optionProcessors;
    }

    @Override
    public void setOptionProcessors(Collection<OptionProcessor> optionProcessors) {
        isNotNull(optionProcessors);
        this.optionProcessors = optionProcessors;
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
    public PrioritySet<Trigger> getTriggers() {
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
    public boolean isCommand(String argument) {
        return false;
    }

    @Override
    public void preProcess(CommandLine commandLine, ListIterator<String> arguments) {
        preProcessOption(commandLine, arguments);
        for (OptionProcessor optionProcessor : getOptionProcessors()) {
            optionProcessor.preProcess(commandLine, this, arguments);
        }
    }

    protected void preProcessOption(CommandLine commandLine, ListIterator<String> arguments) {
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        processOption(commandLine, arguments);
        for (OptionProcessor optionProcessor : getOptionProcessors()) {
            optionProcessor.process(commandLine, this, arguments);
        }
    }

    protected void processOption(CommandLine commandLine, ListIterator<String> arguments) {
    }

    @Override
    public void postProcess(CommandLine commandLine) {
        postProcessOption(commandLine);
        for (OptionProcessor optionProcessor : getOptionProcessors()) {
            optionProcessor.postProcess(commandLine, this);
        }
    }

    protected void postProcessOption(CommandLine commandLine) {
        if (isRequired() && !commandLine.hasOption(this)) {
            postProcessRequired();
        }
    }

    protected void postProcessRequired() {
        optionRequired(this);
    }

    public static void createTriggers(PrioritySet<Trigger> triggers, Set<String> prefixes, String trigger) {
        for (String prefix : prefixes) {
            triggers.add(new TriggerImpl(prefix + trigger));
        }
    }

    public static void join(StringBuilder help, Collection<?> values) {
        for (Iterator<?> iterator = values.iterator(); iterator.hasNext();) {
            help.append(iterator.next());
            if (iterator.hasNext()) {
                help.append(",");
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        OptionBase option = (OptionBase) o;
        if (id != option.id)
            return false;
        if (name != null ? !name.equals(option.name) : option.name != null)
            return false;
        if (description != null ? !description.equals(option.description) : option.description != null)
            return false;
        if (required != option.required)
            return false;
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
        help(help, newHashSet(HelpHint.values()), null);
        return help.toString();
    }
}
