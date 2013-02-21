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
import com.nuodb.migration.cli.parse.option.OptionFormat;
import com.nuodb.migration.utils.PriorityList;

import java.util.*;

/**
 * @author Sergey Bushik
 */
public abstract class CliRunAdapter implements CliRun {

    private final OptionFormat optionFormat;
    private final String command;
    private Option option;

    protected CliRunAdapter(OptionFormat optionFormat, String command) {
        this.optionFormat = optionFormat;
        this.command = command;
    }

    @Override
    public int getId() {
        return initOption().getId();
    }

    @Override
    public void setId(int id) {
        initOption().setId(id);
    }

    @Override
    public String getName() {
        return initOption().getName();
    }

    @Override
    public void setName(String name) {
        initOption().setName(name);
    }

    @Override
    public String getDescription() {
        return initOption().getDescription();
    }

    @Override
    public void setDescription(String description) {
        initOption().setDescription(description);
    }

    @Override
    public boolean isRequired() {
        return initOption().isRequired();
    }

    @Override
    public void setRequired(boolean required) {
        initOption().setRequired(required);
    }

    @Override
    public OptionProcessor getOptionProcessor() {
        return initOption().getOptionProcessor();
    }

    @Override
    public void setOptionProcessor(OptionProcessor optionProcessor) {
        initOption().setOptionProcessor(optionProcessor);
    }

    @Override
    public Set<String> getPrefixes() {
        return initOption().getPrefixes();
    }

    @Override
    public void addTrigger(Trigger trigger) {
        initOption().addTrigger(trigger);
    }

    @Override
    public void addTrigger(Trigger trigger, int priority) {
        initOption().addTrigger(trigger, priority);
    }

    @Override
    public PriorityList<Trigger> getTriggers() {
        return initOption().getTriggers();
    }

    @Override
    public Option findOption(String trigger) {
        return initOption().findOption(trigger);
    }

    @Override
    public Option findOption(Trigger trigger) {
        return initOption().findOption(trigger);
    }

    @Override
    public void defaults(CommandLine commandLine) {
        initOption().defaults(commandLine);
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return initOption().canProcess(commandLine, argument);
    }

    @Override
    public boolean canProcess(CommandLine commandLine, ListIterator<String> arguments) {
        return initOption().canProcess(commandLine, arguments);
    }

    @Override
    public void preProcess(CommandLine commandLine, ListIterator<String> arguments) {
        initOption().preProcess(commandLine, arguments);
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        initOption().process(commandLine, arguments);
    }

    @Override
    public void postProcess(CommandLine commandLine) {
        initOption().postProcess(commandLine);
        bind(commandLine);
    }

    @Override
    public void setOptionFormat(OptionFormat optionFormat) {
        initOption().setOptionFormat(optionFormat);
    }

    protected void bind(OptionSet optionSet) {
    }

    @Override
    public void help(StringBuilder buffer, Collection<HelpHint> hints, Comparator<Option> comparator) {
        initOption().help(buffer, hints, comparator);
    }

    @Override
    public List<Help> help(int indent, Collection<HelpHint> hints, Comparator<Option> comparator) {
        return initOption().help(indent, hints, comparator);
    }

    protected Option initOption() {
        if (option == null) {
            option = createOption();
        }
        return option;
    }

    protected abstract Option createOption();

    @Override
    public String getCommand() {
        return command;
    }

    @Override
    public OptionFormat getOptionFormat() {
        return optionFormat;
    }
}
