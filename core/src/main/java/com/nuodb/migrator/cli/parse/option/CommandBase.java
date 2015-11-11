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
package com.nuodb.migrator.cli.parse.option;

import com.google.common.collect.Lists;
import com.nuodb.migrator.cli.parse.Command;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Help;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.Trigger;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static java.util.Collections.singletonList;

/**
 * @author Sergey Bushik
 */
public abstract class CommandBase extends OptionBase implements Command {

    private Collection<String> commands;
    private Collection<String> helpValues;

    public CommandBase() {
        this(Lists.<String>newArrayList());
    }

    public CommandBase(Collection<String> commands) {
        this.commands = commands;
    }

    @Override
    public Collection<String> getCommands() {
        return commands;
    }

    @Override
    public void setCommands(Collection<String> commands) {
        this.commands = commands;
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
    public PrioritySet<Trigger> getTriggers() {
        PrioritySet<Trigger> triggers = com.nuodb.migrator.utils.Collections.newPrioritySet();
        for (String command : getCommands()) {
            triggers.add(new TriggerImpl(command));
        }
        return triggers;
    }

    @Override
    public Set<String> getPrefixes() {
        return Collections.emptySet();
    }

    @Override
    public void defaults(CommandLine commandLine) {
    }

    @Override
    public boolean isCommand(String argument) {
        return getCommands().contains(argument);
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return isCommand(argument);
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        String command = arguments.next();
        arguments.previous();
        process(commandLine, arguments, command);
    }

    protected abstract void process(CommandLine commandLine, ListIterator<String> arguments, String argument);

    @Override
    public void help(StringBuilder buffer, Collection<HelpHint> hints, Comparator<Option> comparator) {
        Collection<String> values = getHelpValues() != null ? getHelpValues() : getCommands();
        boolean bracketed = hints.contains(HelpHint.ARGUMENT_BRACKETED);
        int i = 0;
        for (String value : values) {
            if (i++ > 0) {
                buffer.append(' ');
            }
            if (bracketed) {
                buffer.append('<');
            }
            buffer.append(value);
            if (bracketed) {
                buffer.append('>');
            }
        }
    }

    @Override
    public List<Help> help(int indent, Collection<HelpHint> hints, Comparator<Option> comparator) {
        return singletonList((Help) new HelpImpl(this, indent));
    }
}
