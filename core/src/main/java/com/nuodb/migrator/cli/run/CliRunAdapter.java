/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.cli.run;

import com.google.common.collect.Maps;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Help;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionProcessor;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.OptionValidator;
import com.nuodb.migrator.cli.parse.Trigger;
import com.nuodb.migrator.cli.parse.option.OptionFormat;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 * @author Sergey Bushik
 */
public abstract class CliRunAdapter extends CliRunSupport implements CliRun {

    private String command;
    private Option option;

    protected CliRunAdapter() {
    }

    protected CliRunAdapter(String command) {
        this.command = command;
    }

    @Override
    public void execute() {
        execute(Maps.<Object, Object>newHashMap());
    }

    @Override
    public int getId() {
        return getOption().getId();
    }

    @Override
    public void setId(int id) {
        getOption().setId(id);
    }

    @Override
    public String getName() {
        return getOption().getName();
    }

    @Override
    public void setName(String name) {
        getOption().setName(name);
    }

    @Override
    public String getDescription() {
        return getOption().getDescription();
    }

    @Override
    public void setDescription(String description) {
        getOption().setDescription(description);
    }

    @Override
    public boolean isRequired() {
        return getOption().isRequired();
    }

    @Override
    public void setRequired(boolean required) {
        getOption().setRequired(required);
    }

    @Override
    public void addOptionValidator(OptionValidator optionValidator) {
        getOption().addOptionValidator(optionValidator);
    }

    @Override
    public void addOptionProcessor(OptionProcessor optionProcessor) {
        getOption().addOptionProcessor(optionProcessor);
    }

    @Override
    public void removeOptionProcessor(OptionProcessor optionProcessor) {
        getOption().removeOptionProcessor(optionProcessor);
    }

    @Override
    public Collection<OptionProcessor> getOptionProcessors() {
        return getOption().getOptionProcessors();
    }

    @Override
    public void setOptionProcessors(Collection<OptionProcessor> optionProcessors) {
        getOption().setOptionProcessors(optionProcessors);
    }

    @Override
    public Set<String> getPrefixes() {
        return getOption().getPrefixes();
    }

    @Override
    public void addTrigger(Trigger trigger) {
        getOption().addTrigger(trigger);
    }

    @Override
    public void addTrigger(Trigger trigger, int priority) {
        getOption().addTrigger(trigger, priority);
    }

    @Override
    public PrioritySet<Trigger> getTriggers() {
        return getOption().getTriggers();
    }

    @Override
    public Option findOption(String trigger) {
        return getOption().findOption(trigger);
    }

    @Override
    public Option findOption(Trigger trigger) {
        return getOption().findOption(trigger);
    }

    @Override
    public void defaults(CommandLine commandLine) {
        getOption().defaults(commandLine);
    }

    @Override
    public boolean isCommand(String argument) {
        return getOption().isCommand(argument);
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return getOption().canProcess(commandLine, argument);
    }

    @Override
    public boolean canProcess(CommandLine commandLine, ListIterator<String> arguments) {
        return getOption().canProcess(commandLine, arguments);
    }

    @Override
    public void preProcess(CommandLine commandLine, ListIterator<String> arguments) {
        getOption().preProcess(commandLine, arguments);
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        getOption().process(commandLine, arguments);
    }

    @Override
    public void postProcess(CommandLine commandLine) {
        getOption().postProcess(commandLine);
        bind(commandLine);
    }

    @Override
    public void setOptionFormat(OptionFormat optionFormat) {
        getOption().setOptionFormat(optionFormat);
    }

    protected void bind(OptionSet optionSet) {
    }

    @Override
    public void help(StringBuilder buffer, Collection<HelpHint> hints, Comparator<Option> comparator) {
        getOption().help(buffer, hints, comparator);
    }

    @Override
    public List<Help> help(int indent, Collection<HelpHint> hints, Comparator<Option> comparator) {
        return getOption().help(indent, hints, comparator);
    }

    @Override
    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    protected Option getOption() {
        if (option == null) {
            option = createOption();
        }
        return option;
    }

    protected abstract Option createOption();
}
