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
package com.nuodb.migration.cli.parse.option;

import com.google.common.collect.Maps;
import com.nuodb.migration.cli.parse.*;
import com.nuodb.migration.match.AntRegexCompiler;
import com.nuodb.migration.match.Match;
import com.nuodb.migration.match.RegexCompiler;
import com.nuodb.migration.utils.PriorityList;
import com.nuodb.migration.utils.PriorityListImpl;

import java.util.*;

import static com.nuodb.migration.cli.parse.HelpHint.CONTAINER_ARGUMENT;
import static com.nuodb.migration.cli.parse.HelpHint.CONTAINER_GROUP;
import static com.nuodb.migration.cli.parse.HelpHint.OPTIONAL;

/**
 * @author Sergey Bushik
 */
public class RegexOption extends ContainerBase {

    /**
     * Regular expression compiler used to compile wild char triggers into a pattern to match & process --table.*,
     * --table.*.filter=<filter> options.
     */
    private RegexCompiler regexCompiler = AntRegexCompiler.INSTANCE;
    private Map<RegexTrigger, Integer> triggersGroups = Maps.newHashMap();

    public RegexOption() {
        setOptionProcessor(new ArgumentMaximumProcessor());
    }

    public RegexOption(int id, String name, String description, boolean required) {
        super(id, name, description, required);
        setOptionProcessor(new ArgumentMaximumProcessor());
    }

    public void addRegex(String regex, int group, int priority) {
        for (String prefix : getPrefixes()) {
            RegexTrigger trigger = new RegexTrigger(regexCompiler.compile(prefix + regex));
            getTriggersGroups().put(trigger, group);
            addTrigger(trigger, priority);
        }
    }

    /**
     * Tests if arguments matches Ant style regexp for provided table names.
     *
     * @param commandLine command line to store matched table names in.
     * @param arguments   to withConnection regular expression on.
     */
    @Override
    public void processInternal(CommandLine commandLine, ListIterator<String> arguments) {
        String argument = arguments.next();
        Trigger trigger = getTriggerFired(getTriggers(), argument);
        if (canProcess(commandLine, argument)) {
            processTriggerFired(commandLine, trigger, argument);
        } else {
            throw new OptionException(this, String.format("Unexpected token %1$s", argument));
        }
    }

    protected void processTriggerFired(CommandLine commandLine, Trigger trigger, String argument) {
        commandLine.addOption(this);
        if (trigger instanceof RegexTrigger) {
            processRegexTriggerFired(commandLine, (RegexTrigger) trigger, argument);
        }
    }

    protected void processRegexTriggerFired(CommandLine commandLine, RegexTrigger trigger, String argument) {
        Match match = trigger.getRegex().exec(argument);
        Integer group = getTriggersGroups().get(trigger);
        String[] matches = match.matches();
        if (group != null && matches.length >= group) {
            commandLine.addValue(this, matches[group]);
        }
    }

    public Map<RegexTrigger, Integer> getTriggersGroups() {
        return triggersGroups;
    }

    @Override
    public void help(StringBuilder help, Set<HelpHint> hints, Comparator<Option> comparator) {
        boolean optional = !isRequired() && hints.contains(OPTIONAL);
        if (optional) {
            help.append('[');
        }
        PriorityList<Trigger> triggers = new PriorityListImpl<Trigger>();
        createTriggers(triggers, getPrefixes(), getName());
        join(help, triggers);

        Argument argument = getArgument();
        boolean displayArgument = argument != null && hints.contains(CONTAINER_ARGUMENT);
        Group group = getGroup();
        boolean displayGroup = group != null && hints.contains(CONTAINER_GROUP);
        if (displayArgument) {
            help.append(getArgumentSeparator());
            boolean bracketed = hints.contains(HelpHint.ARGUMENT_BRACKETED);
            // if the arguments is optional
            if (optional) {
                help.append('[');
            }
            if (bracketed) {
                help.append('<');
            }
            // append name
            help.append(argument.getName());

            if (bracketed) {
                help.append('>');
            }
            if (optional) {
                help.append(']');
            }
        }
        if (displayGroup) {
            help.append(' ');
            group.help(help, hints, comparator);
        }
        if (optional) {
            help.append(']');
        }
    }

    /**
     * Updates maximum value of container's argument option. Used with RegexOption when part of option name matching
     * regular expression is stored in the option items and option's argument.
     */
    static class ArgumentMaximumProcessor implements OptionProcessor {

        private int count = 0;

        @Override
        public void preProcess(CommandLine commandLine, Option option, ListIterator<String> arguments) {
        }

        @Override
        public void process(CommandLine commandLine, Option option, ListIterator<String> arguments) {
            Container container = (Container) option;
            Argument argument = container.getArgument();
            if (argument != null) {
                argument.setMaximum(++count * 2);
            }
        }

        @Override
        public void postProcess(CommandLine commandLine, Option option) {
        }
    }
}
