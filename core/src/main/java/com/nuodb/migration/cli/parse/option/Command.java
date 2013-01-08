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

import com.google.common.collect.Lists;
import com.nuodb.migration.cli.parse.*;
import com.nuodb.migration.utils.PriorityList;
import com.nuodb.migration.utils.PriorityListImpl;

import java.util.*;

import static com.nuodb.migration.cli.parse.HelpHint.ALIASES;
import static com.nuodb.migration.cli.parse.HelpHint.OPTIONAL;

/**
 * @author Sergey Bushik
 */
public class Command extends ContainerBase {

    private Set<String> aliases;

    public Command() {
    }

    public Command(int id, String name, String description, boolean required) {
        super(id, name, description, required);
    }

    public Command(int id, String name, String description, boolean required,
                   Set<String> aliases) {
        super(id, name, description, required);
        this.aliases = aliases;
    }

    public Set<String> getAliases() {
        return aliases;
    }

    public void setAliases(Set<String> aliases) {
        this.aliases = aliases;
    }

    @Override
    public PriorityList<Trigger> getTriggers() {
        PriorityList<Trigger> triggers = new PriorityListImpl<Trigger>();
        triggers.addAll(super.getTriggers());
        triggers.add(new TriggerImpl(getName()));
        Set<String> aliases = getAliases();
        if (aliases != null) {
            for (String alias : aliases) {
                triggers.add(new TriggerImpl(alias));
            }
        }
        return triggers;
    }

    @Override
    public void doProcess(CommandLine commandLine, ListIterator<String> arguments) {
        String argument = arguments.next();
        if (canProcess(commandLine, argument)) {
            commandLine.addOption(this);
        } else {
            throw new OptionException(this, String.format("Unexpected token %1$s", argument));
        }
    }

    @Override
    public void help(StringBuilder buffer, Set<HelpHint> hints, Comparator<Option> comparator) {
        boolean optional = !isRequired() && hints.contains(OPTIONAL);
        boolean displayAliases = hints.contains(ALIASES);
        if (optional) {
            buffer.append('[');
        }
        buffer.append(getName());
        Set<String> aliases = getAliases();
        if (displayAliases && (aliases != null && !aliases.isEmpty())) {
            buffer.append(" (");
            List<String> list = Lists.newArrayList(aliases);
            Collections.sort(list);
            for (Iterator<String> i = list.iterator(); i.hasNext(); ) {
                String alias = i.next();
                buffer.append(alias);
                if (i.hasNext()) {
                    buffer.append(',');
                }
            }
            buffer.append(')');
        }
        super.help(buffer, hints, comparator);
        if (optional) {
            buffer.append(']');
        }
    }
}
