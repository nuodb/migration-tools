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
package com.nuodb.tools.migration.cli.handler.option;

import com.nuodb.tools.migration.cli.handler.*;

import java.util.*;

/**
 * Handles the java style "-Dproperty=value" options
 */
public class SimpleProperty extends BaseOption {

    public static String PREFIX = "-D";

    private String prefix;

    public SimpleProperty() {
        this(0, "Property", null, false);
    }

    public SimpleProperty(int id, String name, String description, boolean required) {
        this(id, name, description, required, PREFIX);
    }

    public SimpleProperty(int id, String name, String description, boolean required, String prefix) {
        super(id, name, description, required);
        this.prefix = prefix;
    }

    @Override
    public Set<String> getPrefixes() {
        return Collections.singleton(prefix);
    }

    @Override
    public Set<String> getTriggers() {
        return Collections.singleton(prefix);
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
            throw new OptionException(this, "Unexpected token " + argument);
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

    @Override
    public void postProcess(CommandLine commandLine) {
    }

    @Override
    public void help(StringBuilder buffer, Set<HelpHint> helpSettings, Comparator<Option> comparator) {
        if (helpSettings.contains(HelpHint.PROPERTY)) {
            boolean bracketed = helpSettings.contains(HelpHint.ARGUMENT_BRACKETED);
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
    public List<Help> help(int indent, Set<HelpHint> hints, Comparator<Option> comparator) {
        if (hints.contains(HelpHint.PROPERTY)) {
            Help help = new SimpleHelp(this, indent);
            return Collections.singletonList(help);
        } else {
            return Collections.emptyList();
        }
    }
}
