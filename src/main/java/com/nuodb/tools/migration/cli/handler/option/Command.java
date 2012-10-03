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
package com.nuodb.tools.migration.cli.handler.option;

import com.nuodb.tools.migration.cli.handler.*;

import java.util.*;

import static com.nuodb.tools.migration.cli.handler.HelpHint.ALIASES;
import static com.nuodb.tools.migration.cli.handler.HelpHint.OPTIONAL;

/**
 * @author Sergey Bushik
 */
public class Command extends BaseParent {

    private Set<String> aliases;

    public Command(int id, String name, String description, boolean required) {
        super(id, name, description, required);
    }

    public Command(int id, String name, String description, boolean required, Set<String> aliases) {
        super(id, name, description, required);
        this.aliases = aliases;
    }

    public Command(int id, String name, String description, boolean required, Group children,
                   Argument argument, String argumentSeparator, Set<String> aliases) {
        super(id, name, description, required, children, argument, argumentSeparator);
        this.aliases = aliases;
    }

    @Override
    public Set<String> getTriggers() {
        Set<String> triggers = new HashSet<String>();
        triggers.add(getName());
        if (aliases != null) {
            triggers.addAll(aliases);
        }
        return Collections.unmodifiableSet(triggers);
    }

    @Override
    public void processParent(CommandLine commandLine, ListIterator<String> arguments) {
        String argument = arguments.next();
        if (canProcess(commandLine, argument)) {
            commandLine.addOption(this);
            arguments.set(getName());
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
        if (displayAliases && (aliases != null && !aliases.isEmpty())) {
            buffer.append(" (");
            List<String> list = new ArrayList<String>(aliases);
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
