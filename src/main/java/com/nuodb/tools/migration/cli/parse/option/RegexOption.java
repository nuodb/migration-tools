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

import com.nuodb.tools.migration.cli.parse.CommandLine;
import com.nuodb.tools.migration.cli.parse.Trigger;
import com.nuodb.tools.migration.match.AntRegexCompiler;
import com.nuodb.tools.migration.match.Match;
import com.nuodb.tools.migration.match.RegexCompiler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Sergey Bushik
 */
public class RegexOption extends OptionImpl {

    /**
     * Regular expression compiler used to compile wild char triggers into a pattern to match & process --table.*,
     * --table.*.filter=<filter> options.
     */
    private RegexCompiler regexCompiler = AntRegexCompiler.INSTANCE;

    private Map<RegexTrigger, Integer> triggersGroups = new HashMap<RegexTrigger, Integer>();

    public RegexOption() {
    }

    public RegexOption(int id, String name, String description, boolean required) {
        super(id, name, description, required);
    }

    public RegexOption(int id, String name, String description, boolean required,
                       Set<String> prefixes, Set<String> aliases) {
        super(id, name, description, required, prefixes, aliases);
    }

    public void addRegex(String regex, int group) {
        for (String prefix : getPrefixes()) {
            RegexTrigger trigger = new RegexTrigger(regexCompiler.compile(prefix + regex));
            triggersGroups.put(trigger, group);
            addTrigger(trigger);
        }
    }

    /**
     * Tests if arguments matches Ant style regexp for provided table names.
     *
     * @param commandLine command line to store matched table names in.
     * @param argument    to execute regular expression on.
     * @param trigger     which was triggered.
     */
    @Override
    protected void doProcess(CommandLine commandLine, Trigger trigger, String argument) {
        super.doProcess(commandLine, trigger, argument);
        if (trigger instanceof RegexTrigger) {
            doProcess(commandLine, (RegexTrigger) trigger, argument);
        }
    }

    protected void doProcess(CommandLine commandLine, RegexTrigger trigger, String argument) {
        Match match = trigger.getRegex().exec(argument);
        Integer group = triggersGroups.get(trigger);
        String[] matches = match.matches();
        if (group != null && matches.length >= group) {
            commandLine.addValue(this, matches[group]);
        }
    }

    public Map<RegexTrigger, Integer> getTriggersGroups() {
        return Collections.unmodifiableMap(triggersGroups);
    }
}
