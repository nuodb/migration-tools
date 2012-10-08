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

import com.nuodb.tools.migration.cli.parse.*;

import java.util.*;

/**
 * @author Sergey Bushik
 */
public abstract class BaseParent extends BaseOption {

    private Group group;
    private Argument argument;
    private String argumentSeparator;

    protected BaseParent(int id, String name, String description, boolean required) {
        super(id, name, description, required);
    }

    protected BaseParent(int id, String name, String description, boolean required,
                         Group group, Argument argument, String argumentSeparator) {
        super(id, name, description, required);
        this.group = group;
        this.argument = argument;
        this.argumentSeparator = argumentSeparator;
    }

    @Override
    public Set<String> getPrefixes() {
        return (group == null) ? Collections.<String>emptySet() : group.getPrefixes();
    }

    @Override
    public Set<String> getTriggers() {
        return (group == null) ? Collections.<String>emptySet() : group.getTriggers();
    }

    @Override
    public boolean canProcess(CommandLine commandLine, String argument) {
        Set<String> triggers = getTriggers();
        if (this.argument != null) {
            if (argumentSeparator != null) {
                int index = argument.indexOf(argumentSeparator);
                if (index > 0) {
                    return triggers.contains(argument.substring(0, index));
                }
            }
        }
        return triggers.contains(argument);
    }

    @Override
    public void defaults(CommandLine commandLine) {
        if (argument != null) {
            argument.defaults(commandLine, this);
        }
        if (group != null) {
            group.defaults(commandLine);
        }
    }

    @Override
    public void process(CommandLine commandLine, ListIterator<String> arguments) {
        prepareArgument(arguments);
        processParent(commandLine, arguments);
        processArgument(commandLine, argument, arguments);
        processGroup(commandLine, group, arguments);
    }

    protected void prepareArgument(ListIterator<String> arguments) {
        if (argumentSeparator != null) {
            String value = arguments.next();
            int index = value.indexOf(argumentSeparator);
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

    private static String quote(String argument) {
        return "\"" + argument + "\"";
    }

    protected abstract void processParent(CommandLine commandLine, ListIterator<String> arguments);

    protected void processArgument(CommandLine commandLine, Argument argument, ListIterator<String> arguments) {
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

    private static String unquote(String argument) {
        if (!argument.startsWith("\"") || !argument.endsWith("\"")) {
            return argument;
        }
        return argument.substring(1, argument.length() - 1);
    }

    protected void processGroup(CommandLine commandLine, Group children, ListIterator<String> arguments) {
        if ((children != null) && children.canProcess(commandLine, arguments)) {
            children.process(commandLine, arguments);
        }
    }

    public void validate(CommandLine commandLine) throws OptionException {
        super.validate(commandLine);
        if (commandLine.hasOption(this)) {
            validateGroup(commandLine, group);
            validateArgument(commandLine, argument);
        }
    }

    protected void validateArgument(CommandLine commandLine, Argument argument) {
        if (argument != null) {
            argument.validate(commandLine, this);
        }
    }

    protected void validateGroup(CommandLine commandLine, Group children) {
        if (children != null) {
            children.validate(commandLine);
        }
    }

    @Override
    public void help(StringBuilder buffer, Set<HelpHint> hints, Comparator<Option> comparator) {
        boolean displayArgument = (this.argument != null) && hints.contains(HelpHint.PARENT_ARGUMENT);
        boolean displayChildren = (this.group != null) && hints.contains(HelpHint.PARENT_CHILDREN);
        if (displayArgument) {
            buffer.append(' ');
            argument.help(buffer, hints, comparator);
        }
        if (displayChildren) {
            buffer.append(' ');
            group.help(buffer, hints, comparator);
        }
    }

    @Override
    public List<Help> help(int indent, Set<HelpHint> hints, Comparator<Option> comparator) {
        List<Help> help = new ArrayList<Help>();
        help.add(new HelpImpl(this, indent));
        Argument argument = getArgument();
        if (hints.contains(HelpHint.PARENT_ARGUMENT) && (argument != null)) {
            help.addAll(argument.help(indent + 1, hints, comparator));
        }
        Group children = getGroup();
        if (hints.contains(HelpHint.PARENT_CHILDREN) && (children != null)) {
            help.addAll(children.help(indent + 1, hints, comparator));
        }
        return help;
    }

    public Argument getArgument() {
        return argument;
    }

    public void setArgument(Argument argument) {
        this.argument = argument;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public String getArgumentSeparator() {
        return argumentSeparator;
    }

    public void setArgumentSeparator(String argumentSeparator) {
        this.argumentSeparator = argumentSeparator;
    }
}
