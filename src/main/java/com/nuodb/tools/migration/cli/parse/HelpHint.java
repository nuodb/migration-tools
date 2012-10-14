/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License = new DisplayHint(); Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing = new DisplayHint(); software
 * distributed under the License is distributed on an "AS IS" BASIS = new DisplayHint();
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND = new DisplayHint(); either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nuodb.tools.migration.cli.parse;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HelpHint {
    /**
     * Indicates that aliases should be displayed
     */
    public static final HelpHint ALIASES = new HelpHint();
    /**
     * Indicates that optionality should be included
     */
    public static final HelpHint OPTIONAL = new HelpHint();
    /**
     * Indicates that optional child groups should be displayed in square brackets
     */
    public static final HelpHint OPTIONAL_CHILD_GROUP = new HelpHint();
    /**
     * Indicates that property options should be included
     */
    public static final HelpHint PROPERTY = new HelpHint();
    /**
     * Indicates that switches should be included enabled
     */
    public static final HelpHint SWITCH = new HelpHint();
    /**
     * Indicates that option names should be included
     */
    public static final HelpHint GROUP = new HelpHint();
    /**
     * Indicates that groups should be included expanded
     */
    public static final HelpHint GROUP_OPTIONS = new HelpHint();
    /**
     * Indicates that option arguments should be included
     */
    public static final HelpHint GROUP_ARGUMENTS = new HelpHint();
    /**
     * Indicates that option outer brackets should be included
     */
    public static final HelpHint GROUP_OUTER = new HelpHint();
    /**
     * Indicates that arguments should be included numbered
     */
    public static final HelpHint ARGUMENT_NUMBERED = new HelpHint();
    /**
     * Indicates that arguments should be included bracketed
     */
    public static final HelpHint ARGUMENT_BRACKETED = new HelpHint();
    /**
     * Indicates that arguments of parents should be included
     */
    public static final HelpHint CONTAINER_ARGUMENT = new HelpHint();
    /**
     * Indicates that children of parents should be included
     */
    public static final HelpHint CONTAINER_GROUP = new HelpHint();

    public static Set<HelpHint> ALL_HINTS;

    static {
        Set<HelpHint> allHints = new HashSet<HelpHint>();
        for (Field field : HelpHint.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) &&
                    field.getType().equals(HelpHint.class)) {
                try {
                    allHints.add((HelpHint) field.get(null));
                } catch (IllegalAccessException e) {
                    // ignore and continue iterating
                }
            }
        }
        ALL_HINTS = Collections.unmodifiableSet(allHints);
    }
}
