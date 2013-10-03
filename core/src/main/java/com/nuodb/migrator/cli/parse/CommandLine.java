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

import java.util.List;

/**
 * A CommandLine that detected items and options can be written to.
 */
public interface CommandLine extends OptionSet {

    List<String> getArguments();

    void addOption(Option option);

    void addValue(Option option, Object value);

    void addSwitch(Option option, boolean value);

    void addProperty(Option option, String property, String value);

    void addProperty(String property, String value);

    void setDefaultValues(Option option, List<Object> defaultValues);

    void setDefaultSwitch(Option option, Boolean defaultSwitch);

    List<Object> getValues(Option option);

    boolean isOption(String argument);

    boolean isCommand(String argument);
}
