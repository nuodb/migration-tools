/**
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

import com.nuodb.migrator.cli.parse.Help;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;

import java.util.Comparator;
import java.util.Set;

/**
 * Represents a line in the help screen.
 */
public class HelpImpl implements Help {

    /**
     * The option that this help describes
     */
    private final Option option;

    /**
     * The level of indentation for this item
     */
    private final int indent;

    /**
     * Creates a new HelpLineImpl to represent a particular Option in the online
     * help.
     *
     * @param option
     *            Option that the HelpLineImpl describes
     * @param indent
     *            Level of indentation for this line
     */
    public HelpImpl(Option option, int indent) {
        this.option = option;
        this.indent = indent;
    }

    /**
     * @return The level of indentation for this line
     */
    public int getIndent() {
        return indent;
    }

    /**
     * @return The Option that the help line relates to
     */
    public Option getOption() {
        return option;
    }

    /**
     * Builds a help string for the option using the specified settings and
     * comparator.
     *
     * @param hints
     *            the settings to apply
     * @param comparator
     *            a comparator to sort options when applicable
     * @return the help string
     */
    public String help(Set<HelpHint> hints, Comparator<Option> comparator) {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < indent; ++i) {
            buffer.append("   ");
        }
        option.help(buffer, hints, comparator);
        return buffer.toString();
    }
}
