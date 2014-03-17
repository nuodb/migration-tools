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

import static com.nuodb.migrator.cli.parse.HelpHint.ARGUMENT_BRACKETED;
import static com.nuodb.migrator.cli.parse.HelpHint.PROPERTY;
import static com.nuodb.migrator.cli.parse.option.OptionUtils.optionUnexpected;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Handles the java style options prefixed with -D
 */
public class Property extends OptionBase {

    public static String PREFIX = "-D";

    private String prefix = PREFIX;

    public Property() {
    }

    public Property(int id, String name, String description, boolean required) {
        super(id, name, description, required);
    }

    public Property(int id, String name, String description, boolean required, String prefix) {
        super(id, name, description, required);
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Set<String> getPrefixes() {
        return Collections.singleton(prefix);
    }

    @Override
    public PrioritySet<Trigger> getTriggers() {
        PrioritySet<Trigger> triggers = com.nuodb.migrator.utils.Collections.newPrioritySet();
        triggers.add(new TriggerImpl(prefix));
        return triggers;
    }

    @Override
    public void defaults(CommandLine commandLine) {
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return argument != null && argument.startsWith(prefix);
    }

    @Override
    public void process(CommandLine line, ListIterator<String> arguments) {
        String argument = arguments.next();
        if (!canProcess(line, argument)) {
            processUnexpected(argument);
        }
        int startIndex = prefix.length();
        int equalsIndex = argument.indexOf('=', startIndex);
        String property;
        String value;
        if (equalsIndex < 0) {
            property = argument.substring(startIndex);
            value = null;
        } else {
            property = argument.substring(startIndex, equalsIndex);
            value = argument.substring(equalsIndex + 1);
        }
        line.addProperty(this, property, value);
    }

    protected void processUnexpected(String argument) {
        optionUnexpected(this, argument);
    }

    @Override
    public void help(StringBuilder buffer, Collection<HelpHint> helpSettings, Comparator<Option> comparator) {
        if (helpSettings.contains(PROPERTY)) {
            boolean bracketed = helpSettings.contains(ARGUMENT_BRACKETED);
            buffer.append(prefix);
            if (bracketed) {
                buffer.append('<');
            }
            buffer.append("property");
            if (bracketed) {
                buffer.append('>');
            }
            buffer.append("=");
            if (bracketed) {
                buffer.append('<');
            }
            buffer.append("value");
            if (bracketed) {
                buffer.append('>');
            }
        }
    }

    @Override
    public List<Help> help(int indent, Collection<HelpHint> hints, Comparator<Option> comparator) {
        if (hints.contains(PROPERTY)) {
            Help help = new HelpImpl(this, indent);
            return singletonList(help);
        } else {
            return emptyList();
        }
    }
}
