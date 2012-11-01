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
package com.nuodb.tools.migration.cli.parse.option;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.nuodb.tools.migration.cli.parse.*;
import com.nuodb.tools.migration.utils.PriorityList;
import com.nuodb.tools.migration.utils.PriorityListImpl;

import java.util.*;

/**
 * @author Sergey Bushik
 */
public abstract class ContainerBase extends OptionBase implements Container {

    private Group group;
    private Argument argument;
    private String argumentSeparator;
    private Set<String> prefixes;

    protected ContainerBase() {
    }

    protected ContainerBase(int id, String name, String description, boolean required) {
        super(id, name, description, required);
    }

    @Override
    public Group getGroup() {
        return group;
    }

    @Override
    public void setGroup(Group group) {
        this.group = group;
    }

    @Override
    public Argument getArgument() {
        return argument;
    }

    @Override
    public void setArgument(Argument argument) {
        this.argument = argument;
    }

    @Override
    public String getArgumentSeparator() {
        return argumentSeparator;
    }

    @Override
    public void setArgumentSeparator(String argumentSeparator) {
        this.argumentSeparator = argumentSeparator;
    }

    @Override
    public Set<String> getPrefixes() {
        Set<String> prefixesOfContainer = Sets.newHashSet();
        if (group != null) {
            prefixesOfContainer.addAll(group.getPrefixes());
        }
        if (prefixes != null) {
            prefixesOfContainer.addAll(prefixes);
        }
        return prefixesOfContainer;
    }

    public void setPrefixes(Set<String> prefixes) {
        this.prefixes = prefixes;
    }

    @Override
    public PriorityList<Trigger> getTriggers() {
        PriorityList<Trigger> triggers = new PriorityListImpl<Trigger>();
        triggers.addAll(super.getTriggers());
        if (group != null) {
            triggers.addAll(group.getTriggers());
        }
        return triggers;
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        PriorityList<Trigger> triggers = getTriggers();
        if (this.argument != null) {
            if (this.argumentSeparator != null) {
                int index = argument.indexOf(this.argumentSeparator);
                if (index > 0) {
                    return getTriggerFired(triggers, argument.substring(0, index)) != null;
                }
            }
        }
        return getTriggerFired(triggers, argument) != null;
    }

    protected Trigger getTriggerFired(PriorityList<Trigger> triggers, String argument) {
        for (Trigger trigger : triggers) {
            if (trigger.fire(argument)) {
                return trigger;
            }
        }
        return null;
    }

    @Override
    public void defaults(CommandLine commandLine) {
        if (this.argument != null) {
            this.argument.defaults(commandLine, this);
        }
        if (this.group != null) {
            this.group.defaults(commandLine);
        }
    }

    @Override
    protected void preProcessInternal(CommandLine commandLine, ListIterator<String> arguments) {
        if (this.argumentSeparator != null) {
            String value = arguments.next();
            int index = value.indexOf(this.argumentSeparator);
            if (index > 0) {
                arguments.remove();
                arguments.add(value.substring(0, index));
                String quoted = quote(value.substring(index + 1));
                arguments.add(quoted);
                arguments.previous();
            }
            arguments.previous();
        }
    }

    public static String quote(String argument) {
        return "\"" + argument + "\"";
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        processInternal(commandLine, arguments);
        OptionProcessor optionProcessor = getOptionProcessor();
        if (optionProcessor != null) {
            optionProcessor.process(commandLine, this, arguments);
        }
        processArgument(commandLine, arguments);
        processGroup(commandLine, arguments);
    }

    protected void processArgument(CommandLine commandLine, ListIterator<String> arguments) {
        if (argument != null && arguments.hasNext()) {
            String value = arguments.next();
            String unquoted = unquote(value);
            if (!value.equals(unquoted)) {
                arguments.remove();
                arguments.add(unquoted);
            }
            arguments.previous();
            argument.process(commandLine, arguments, this);
        }
    }

    protected void processGroup(CommandLine commandLine, ListIterator<String> arguments) {
        if ((group != null) && group.canProcess(commandLine, arguments)) {
            group.process(commandLine, arguments);
        }
    }

    public static String unquote(String argument) {
        if (!argument.startsWith("\"") || !argument.endsWith("\"")) {
            return argument;
        }
        return argument.substring(1, argument.length() - 1);
    }

    @Override
    public void postProcess(CommandLine commandLine) throws OptionException {
        OptionProcessor optionProcessor = getOptionProcessor();
        if (optionProcessor != null) {
            optionProcessor.postProcess(commandLine, this);
        }
        postProcessInternal(commandLine);
        if (commandLine.hasOption(this)) {
            postProcessArgument(commandLine);
            postProcessGroup(commandLine);
        }
    }

    protected void postProcessArgument(CommandLine commandLine) {
        if (argument != null) {
            argument.postProcess(commandLine, this);
        }
    }

    protected void postProcessGroup(CommandLine commandLine) {
        if (group != null) {
            group.postProcess(commandLine);
        }
    }

    @Override
    public void help(StringBuilder buffer, Set<HelpHint> hints, Comparator<Option> comparator) {
        boolean displayArgument = (this.argument != null) && hints.contains(HelpHint.CONTAINER_ARGUMENT);
        boolean displayGroup = (this.group != null) && hints.contains(HelpHint.CONTAINER_GROUP);
        if (displayArgument) {
            buffer.append(' ');
            this.argument.help(buffer, hints, comparator);
        }
        if (displayGroup) {
            buffer.append(' ');
            this.group.help(buffer, hints, comparator);
        }
    }

    @Override
    public List<Help> help(int indent, Set<HelpHint> hints, Comparator<Option> comparator) {
        List<Help> help = Lists.newArrayList();
        help.add(new HelpImpl(this, indent));
        Argument argument = getArgument();
        if (hints.contains(HelpHint.CONTAINER_ARGUMENT) && (argument != null)) {
            help.addAll(argument.help(indent + 1, hints, comparator));
        }
        Group children = getGroup();
        if (hints.contains(HelpHint.CONTAINER_GROUP) && (children != null)) {
            help.addAll(children.help(indent + 1, hints, comparator));
        }
        return help;
    }
}
