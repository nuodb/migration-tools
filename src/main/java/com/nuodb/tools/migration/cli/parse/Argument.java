/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * the ASF licenses this file to You under the Apache License, Version 2.0
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
package com.nuodb.tools.migration.cli.parse;

import java.util.List;
import java.util.ListIterator;

/**
 * An option that can execute items passed on the executable line in the form "--file readme.txt".
 */
public interface Argument extends Option {

    int getMinimum();

    void setMinimum(int minimum);

    int getMaximum();

    void setMaximum(int maximum);

    List<Object> getDefaultValues();

    void setDefaultValues(List<Object> defaultValues);

    String getValuesSeparator();

    void setValuesSeparator(String valuesSeparator);

    /**
     * Adds defaults to a executable line.
     *
     * @param commandLine the executable line object to store defaults in.
     * @param option      the option to store the defaults against.
     */
    void defaults(CommandLine commandLine, Option option);

    /**
     * Processes the style element of the argument.
     * <p/>
     * Values identified should be added to the executable line object in association with this argument.
     *
     * @param commandLine the executable line object to store results in.
     * @param arguments   the arguments.properties to execute.
     * @param option      the option to register value against.
     */
    void process(CommandLine commandLine, ListIterator<String> arguments, Option option);

    /**
     * Performs any post execute logic on the items added to the executable line.
     * <p/>
     *
     * @param commandLine the executable line object to query.
     * @param option      the option to lookup items with.
     */
    void postProcess(CommandLine commandLine, Option option);
}
