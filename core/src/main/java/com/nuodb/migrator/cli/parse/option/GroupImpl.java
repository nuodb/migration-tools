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

import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.CommandLine;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Help;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.Trigger;
import com.nuodb.migrator.utils.PrioritySet;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.cli.parse.HelpHint.*;
import static com.nuodb.migrator.cli.parse.option.OptionUtils.groupMaximum;
import static com.nuodb.migrator.cli.parse.option.OptionUtils.groupMinimum;
import static com.nuodb.migrator.utils.Collections.newPrioritySet;

/**
 * An implementation of the group of options.
 */
public class GroupImpl extends OptionBase implements Group {

    public static final String OPTION_SEPARATOR = "|";

    private int minimum;
    private int maximum;

    private List<Option> options = newArrayList();
    private Set<String> prefixes = newHashSet();
    private List<Argument> arguments = newArrayList();
    private PrioritySet<Trigger> triggers = newPrioritySet();

    public GroupImpl() {
    }

    public GroupImpl(int id, String name, String description, boolean required) {
        super(id, name, description, required);
    }

    public GroupImpl(int id, String name, String description, boolean required, int minimum, int maximum,
            List<Option> options) {
        super(id, name, description, required);
        this.minimum = minimum;
        this.maximum = maximum;
        addOptions(options);
    }

    @Override
    public int getMinimum() {
        return minimum;
    }

    @Override
    public void setMinimum(int minimum) {
        this.minimum = minimum;
    }

    @Override
    public int getMaximum() {
        return maximum;
    }

    @Override
    public void setMaximum(int maximum) {
        this.maximum = maximum;
    }

    @Override
    public void addOption(Option option) {
        if (option instanceof Argument) {
            arguments.add((Argument) option);
        } else {
            options.add(option);
            triggers.addAll(option.getTriggers());
            prefixes.addAll(option.getPrefixes());
        }
    }

    @Override
    public void addOptions(Collection<Option> options) {
        for (Option option : options) {
            addOption(option);
        }
    }

    @Override
    public Collection<Option> getOptions() {
        return options;
    }

    @Override
    public boolean isCommand(String argument) {
        for (Option option : options) {
            if (option.isCommand(argument)) {
                return true;
            }
        }
        return false;
    }

    protected boolean isOption(CommandLine commandLine, String argument) {
        if (argument == null) {
            return false;
        }
        Trigger trigger = new TriggerImpl(argument);
        if (triggers.contains(trigger)) {
            return true;
        }
        for (Option option : options) {
            if (option.canProcess(commandLine, argument)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Option findOption(String trigger) {
        for (Option option : this.options) {
            option = option.findOption(trigger);
            if (option != null) {
                return option;
            }
        }
        return null;
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        return isOption(commandLine, argument) || arguments.size() > 0;
    }

    @Override
    public Set<String> getPrefixes() {
        return prefixes;
    }

    @Override
    public PrioritySet<Trigger> getTriggers() {
        return triggers;
    }

    /**
     * Tests whether this option is required. For groups we evaluate the
     * <code>required</code> flag common to all options, but also take the
     * minimum constraints into account.
     *
     * @return a flag whether this option is required
     */
    @Override
    public boolean isRequired() {
        return super.isRequired() || getMinimum() > 0;
    }

    @Override
    public void defaults(CommandLine commandLine) {
        for (Option option : this.options) {
            option.defaults(commandLine);
        }
        for (Argument argument : this.arguments) {
            argument.defaults(commandLine);
        }
    }

    @Override
    protected void preProcessOption(CommandLine commandLine, ListIterator<String> arguments) {
        for (Option option : this.options) {
            if (option.canProcess(commandLine, arguments)) {
                option.preProcess(commandLine, arguments);
                break;
            }
        }
        for (Argument argument : this.arguments) {
            if (argument.canProcess(commandLine, arguments)) {
                argument.preProcess(commandLine, arguments);
            }
        }
    }

    @Override
    @SuppressWarnings("StringEquality")
    protected void processOption(CommandLine commandLine, ListIterator<String> arguments) {
        String previous = null;
        while (arguments.hasNext()) {
            // grab the next argument
            String argument = arguments.next();
            arguments.previous();
            if (argument == previous) {
                // rollback and abort
                break;
            }
            if (isOption(commandLine, argument) || isCommand(argument)) {
                processOption(commandLine, arguments, argument);
            } else {
                processArgument(commandLine, arguments, argument);
            }
            // remember last processed instance
            previous = argument;
        }
    }

    protected void processOption(CommandLine commandLine, ListIterator<String> arguments, String argument) {
        for (Option option : this.options) {
            if (option.canProcess(commandLine, argument)) {
                option.preProcess(commandLine, arguments);
                option.process(commandLine, arguments);
                break;
            }
        }
    }

    protected void processArgument(CommandLine commandLine, ListIterator<String> arguments, String argument) {
        for (Option option : this.arguments) {
            if (option.canProcess(commandLine, argument)) {
                option.preProcess(commandLine, arguments);
                option.process(commandLine, arguments);
                break;
            }
        }
    }

    @Override
    protected void postProcessOption(CommandLine commandLine) {
        // number of options found
        int present = 0;
        // reference to first unexpected option
        Option unexpected = null;
        for (Option option : this.options) {
            // if the child option is present then post process it
            if (commandLine.hasOption(option)) {
                present++;
                if (maximum != 0 && present > this.maximum) {
                    unexpected = option;
                    break;
                }
            }

        }
        if (present > 0 || isRequired()) {
            for (Option option : this.options) {
                option.postProcess(commandLine);
            }
        }
        // too many options
        if (unexpected != null) {
            postProcessMaximum(unexpected);
        }
        // too few options
        if (present < this.minimum) {
            postProcessMinimum();
        }
        // post process each arguments
        for (Argument argument : arguments) {
            argument.postProcess(commandLine);
        }
    }

    protected void postProcessMinimum() {
        groupMinimum(this);
    }

    protected void postProcessMaximum(Option option) {
        groupMaximum(this, option);
    }

    @Override
    public void help(StringBuilder buffer, Collection<HelpHint> hints, Comparator<Option> comparator) {
        help(buffer, hints, comparator, OPTION_SEPARATOR);
    }

    @Override
    public void help(StringBuilder buffer, Collection<HelpHint> hints, Comparator<Option> comparator,
            String separator) {
        hints = newHashSet(hints);
        boolean optional = !isRequired() && (hints.contains(OPTIONAL) || hints.contains(OPTIONAL_CHILD_GROUP));
        boolean expanded = (getName() == null) || hints.contains(GROUP_OPTIONS);
        boolean named = !expanded || ((getName() != null) && hints.contains(GROUP));
        boolean arguments = hints.contains(GROUP_ARGUMENTS);
        boolean outer = hints.contains(GROUP_OUTER);
        hints.remove(GROUP_OUTER);

        boolean both = named && expanded;
        if (optional) {
            buffer.append('[');
        }
        if (named) {
            buffer.append(getName());
        }
        if (both) {
            buffer.append(" (");
        }
        if (expanded) {
            Set<HelpHint> optionHints;
            if (!hints.contains(GROUP_OPTIONS)) {
                optionHints = Collections.emptySet();
            } else {
                optionHints = newHashSet(hints);
                optionHints.remove(OPTIONAL);
            }
            // grab a list of the option's options.
            List<Option> list;
            if (comparator == null) {
                // default to using the initial order
                list = options;
            } else {
                // sort options if comparator is supplied
                list = newArrayList(options);
                Collections.sort(list, comparator);
            }
            // for each option.
            for (Iterator i = list.iterator(); i.hasNext();) {
                Option option = (Option) i.next();

                // append help information
                option.help(buffer, optionHints, comparator);

                // add separator as needed
                if (i.hasNext()) {
                    buffer.append(separator);
                }
            }
        }
        if (both) {
            buffer.append(')');
        }
        if (optional && outer) {
            buffer.append(']');
        }
        if (arguments) {
            for (Argument argument : this.arguments) {
                buffer.append(separator);
                argument.help(buffer, hints, comparator);
            }
        }
        if (optional && !outer) {
            buffer.append(']');
        }
    }

    @Override
    public List<Help> help(int indent, Collection<HelpHint> hints, Comparator<Option> comparator) {
        List<Help> help = newArrayList();
        if (hints.contains(GROUP)) {
            help.add(new HelpImpl(this, indent));
        }
        if (hints.contains(GROUP_OPTIONS)) {
            // grab a list of the option's options.
            List<Option> options;
            if (comparator == null) {
                // default to using the initial order
                options = this.options;
            } else {
                // sort options if comparator is supplied
                options = newArrayList(this.options);
                Collections.sort(options, comparator);
            }
            // for each option
            for (Option option : options) {
                help.addAll(option.help(indent + 1, hints, comparator));
            }
        }
        if (hints.contains(GROUP_ARGUMENTS)) {
            for (Argument argument : this.arguments) {
                help.addAll(argument.help(indent + 1, hints, comparator));
            }
        }
        return help;
    }
}