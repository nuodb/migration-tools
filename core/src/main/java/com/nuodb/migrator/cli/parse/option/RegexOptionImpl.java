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

import com.google.common.collect.Maps;
import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.RegexOption;
import com.nuodb.migrator.cli.parse.Trigger;
import com.nuodb.migrator.match.AntRegexCompiler;
import com.nuodb.migrator.match.Match;
import com.nuodb.migrator.match.Regex;
import com.nuodb.migrator.match.RegexCompiler;
import com.nuodb.migrator.utils.Collections;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;
import java.util.Comparator;
import java.util.ListIterator;
import java.util.Map;

import static com.nuodb.migrator.cli.parse.HelpHint.*;
import static com.nuodb.migrator.cli.parse.option.OptionUtils.optionUnexpected;

/**
 * @author Sergey Bushik
 */
public class RegexOptionImpl extends AugmentOptionBase implements RegexOption {

    /**
     * Regular expression compiler used to compile wild char triggers into a
     * pattern to match & process --table.*, --table.*.filter=<filter> options.
     */
    private RegexCompiler regexCompiler = AntRegexCompiler.INSTANCE;
    private Map<RegexTrigger, Integer> triggersGroups = Maps.newHashMap();

    @Override
    public void setArgument(Argument argument) {
        super.setArgument(argument);
        if (argument != null) {
            argument.addOptionProcessor(new RegexOptionProcessor());
        }
    }

    @Override
    public void addRegex(String regex, int group, int priority) {
        for (String prefix : getPrefixes()) {
            addRegex(regexCompiler.compile(prefix + regex), group, priority);
        }
    }

    @Override
    public void addRegex(Regex regex, int group, int priority) {
        RegexTrigger trigger = new RegexTrigger(regex);
        getTriggersGroups().put(trigger, group);
        addTrigger(trigger, priority);
    }

    @Override
    public RegexCompiler getRegexCompiler() {
        return regexCompiler;
    }

    @Override
    public void setRegexCompiler(RegexCompiler regexCompiler) {
        this.regexCompiler = regexCompiler;
    }

    /**
     * Tests if arguments matches Ant style regexp for provided table names.
     *
     * @param commandLine
     *            command line to store matched table names in.
     * @param arguments
     *            to withConnection regular expression on.
     */
    @Override
    public void processOption(CommandLine commandLine, ListIterator<String> arguments) {
        String argument = arguments.next();
        Trigger trigger = findTrigger(getTriggers(), argument);
        if (canProcess(commandLine, argument)) {
            processOption(commandLine, trigger, argument);
        } else {
            processUnexpected(argument);
        }
    }

    protected void processUnexpected(String argument) {
        optionUnexpected(this, argument);
    }

    protected void processOption(CommandLine commandLine, Trigger trigger, String argument) {
        commandLine.addOption(this);
        if (trigger instanceof RegexTrigger) {
            processRegexOption(commandLine, (RegexTrigger) trigger, argument);
        }
    }

    protected void processRegexOption(CommandLine commandLine, RegexTrigger trigger, String argument) {
        Match match = trigger.getRegex().exec(argument);
        Integer group = getTriggersGroups().get(trigger);
        String[] matches = match.matches();
        if (group != null && matches.length >= group) {
            commandLine.addValue(this, matches[group]);
        }
    }

    protected Map<RegexTrigger, Integer> getTriggersGroups() {
        return triggersGroups;
    }

    @Override
    public void help(StringBuilder help, Collection<HelpHint> hints, Comparator<Option> comparator) {
        boolean optional = !isRequired() && hints.contains(OPTIONAL);
        if (optional) {
            help.append('[');
        }
        PrioritySet<Trigger> triggers = Collections.newPrioritySet();
        createTriggers(triggers, getPrefixes(), getName());
        join(help, triggers);

        Argument argument = getArgument();
        boolean displayArgument = argument != null && hints.contains(AUGMENT_ARGUMENT);
        Group group = getGroup();
        boolean displayGroup = group != null && hints.contains(AUGMENT_GROUP);
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
}
