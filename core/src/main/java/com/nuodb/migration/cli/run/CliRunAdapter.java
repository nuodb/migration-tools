/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.migration.cli.run;

import com.nuodb.migration.cli.parse.*;
import com.nuodb.migration.utils.PriorityList;

import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 * @author Sergey Bushik
 */
public abstract class CliRunAdapter implements CliRun {

    private Option option;
    private String command;

    protected CliRunAdapter(String command) {
        this.command = command;
        this.option = createOption();
    }

    protected abstract Option createOption();

    @Override
    public int getId() {
        return option.getId();
    }

    @Override
    public void setId(int id) {
        option.setId(id);
    }

    @Override
    public String getName() {
        return option.getName();
    }

    @Override
    public void setName(String name) {
        option.setName(name);
    }

    @Override
    public String getDescription() {
        return option.getDescription();
    }

    @Override
    public void setDescription(String description) {
        option.setDescription(description);
    }

    @Override
    public boolean isRequired() {
        return option.isRequired();
    }

    @Override
    public void setRequired(boolean required) {
        option.setRequired(required);
    }

    @Override
    public OptionProcessor getOptionProcessor() {
        return option.getOptionProcessor();
    }

    @Override
    public void setOptionProcessor(OptionProcessor optionProcessor) {
        option.setOptionProcessor(optionProcessor);
    }

    @Override
    public Set<String> getPrefixes() {
        return option.getPrefixes();
    }

    @Override
    public void addTrigger(Trigger trigger) {
        option.addTrigger(trigger);
    }

    @Override
    public void addTrigger(Trigger trigger, int priority) {
        option.addTrigger(trigger, priority);
    }

    @Override
    public PriorityList<Trigger> getTriggers() {
        return option.getTriggers();
    }

    @Override
    public Option findOption(String trigger) {
        return option.findOption(trigger);
    }

    @Override
    public Option findOption(Trigger trigger) {
        return option.findOption(trigger);
    }

    @Override
    public void defaults(CommandLine commandLine) {
        option.defaults(commandLine);
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return option.canProcess(commandLine, argument);
    }

    @Override
    public boolean canProcess(CommandLine commandLine, ListIterator<String> arguments) {
        return option.canProcess(commandLine, arguments);
    }

    @Override
    public void preProcess(CommandLine commandLine, ListIterator<String> arguments) {
        option.preProcess(commandLine, arguments);
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        option.process(commandLine, arguments);
    }

    @Override
    public void postProcess(CommandLine commandLine) {
        option.postProcess(commandLine);
        bind(commandLine);
    }

    protected void bind(CommandLine commandLine) {
    }

    @Override
    public void help(StringBuilder buffer, Set<HelpHint> hints, Comparator<Option> comparator) {
        option.help(buffer, hints, comparator);
    }

    @Override
    public List<Help> help(int indent, Set<HelpHint> hints, Comparator<Option> comparator) {
        return option.help(indent, hints, comparator);
    }

    public Option getOption() {
        return option;
    }

    public void setOption(Option option) {
        this.option = option;
    }

    @Override
    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }
}
