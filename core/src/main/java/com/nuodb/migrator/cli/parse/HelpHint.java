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
package com.nuodb.migrator.cli.parse;

public enum HelpHint {
    /**
     * Indicates that aliases should be displayed
     */
    ALIASES,
    /**
     * Indicates that optionality should be included
     */
    OPTIONAL,
    /**
     * Indicates that optional child groups should be displayed in square
     * brackets
     */
    OPTIONAL_CHILD_GROUP,
    /**
     * Indicates that property options should be included
     */
    PROPERTY,
    /**
     * Indicates that switches should be included enabled
     */
    SWITCH,
    /**
     * Indicates that option names should be included
     */
    GROUP,
    /**
     * Indicates that groups should be included expanded
     */
    GROUP_OPTIONS,
    /**
     * Indicates that option arguments should be included
     */
    GROUP_ARGUMENTS,
    /**
     * Indicates that option outer brackets should be included
     */
    GROUP_OUTER,
    /**
     * Indicates that arguments should be included numbered
     */
    ARGUMENT_NUMBERED,
    /**
     * Indicates that arguments should be included bracketed
     */
    ARGUMENT_BRACKETED,
    /**
     * Indicates that arguments of parents should be included
     */
    AUGMENT_ARGUMENT,
    /**
     * Indicates that children of parents should be included
     */
    AUGMENT_GROUP
}
