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
package com.nuodb.migrator.cli.parse;

import java.util.Collection;
import java.util.Comparator;

/**
 * An option representing a choice or option of options in the form "-a|-b|-c".
 */
public interface Group extends Option {

    /**
     * Retrieves the minimum number of members required for a valid option
     *
     * @return the minimum number of members
     */
    int getMinimum();

    void setMinimum(int minimum);

    /**
     * Retrieves the maximum number of members acceptable for a valid option
     *
     * @return the maximum number of members
     */
    int getMaximum();

    void setMaximum(int maximum);

    /**
     * Adds option to this option
     *
     * @param option
     *            to be added
     */
    void addOption(Option option);

    /**
     * Adds all the options from specified list to this option
     *
     * @param options
     *            list of options
     */
    void addOptions(Collection<Option> options);

    /**
     * Returns list of child options
     *
     * @return child options
     */
    Collection<Option> getOptions();

    /**
     * Appends help information to the specified buffer
     *
     * @param buffer
     *            the buffer to append to
     * @param hints
     *            a setValue of display hints
     * @param comparator
     *            a comparator used to sort the options
     * @param separator
     *            the string used to separate member options
     */
    void help(StringBuilder buffer, Collection<HelpHint> hints, Comparator<Option> comparator, String separator);
}
