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
package com.nuodb.migration.cli.parse.parser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.nuodb.migration.cli.parse.CommandLine;
import com.nuodb.migration.cli.parse.Option;
import com.nuodb.migration.cli.parse.Trigger;
import com.nuodb.migration.cli.parse.option.Property;
import com.nuodb.migration.cli.parse.option.TriggerImpl;

import java.util.*;

/**
 * A executable line implementation allowing options to row their processed information to a CommandLine.
 */
public class CommandLineImpl extends OptionSetImpl implements CommandLine {

    private List<String> arguments;
    private Set<Option> options = Sets.newHashSet();
    private Map<Option, List<Object>> values = Maps.newHashMap();
    private Map<Option, Boolean> switches = Maps.newHashMap();
    private Map<Option, List<Object>> defaultValues = Maps.newHashMap();
    private Map<Option, Boolean> defaultSwitches = Maps.newHashMap();
    private Map<Option, Properties> properties = Maps.newHashMap();
    private Option root;
    private Option current;

    /**
     * Creates a new WriteableCommandLineImpl rooted on the specified option, to hold the parsed withConnection.arguments.
     *
     * @param root      the executable line's root option
     * @param arguments the withConnection.arguments this executable line represents
     */
    public CommandLineImpl(Option root, List<String> arguments) {
        this.root = root;
        this.arguments = arguments;
        this.current = root;
    }

    @Override
    public boolean hasOption(Option option) {
        return options.contains(option);
    }

    @Override
    public void addOption(Option option) {
        options.add(option);
    }

    @Override
    public void addValue(Option option, Object value) {
        addOption(option);
        List<Object> values = this.values.get(option);
        if (values == null) {
            values = Lists.newArrayList();
            this.values.put(option, values);
        }
        values.add(value);
    }

    @Override
    public void addSwitch(Option option, boolean value) {
        addOption(option);
        this.switches.put(option, value ? Boolean.TRUE : Boolean.FALSE);
    }

    @Override
    public void addProperty(Option option, String property, String value) {
        Properties properties = this.properties.get(option);
        if (properties == null) {
            properties = new Properties();
            this.properties.put(option, properties);
        }
        properties.setProperty(property, value);
    }

    @Override
    public void addProperty(String property, String value) {
        addProperty(new Property(), property, value);
    }

    @Override
    public void setDefaultValues(Option option, List<Object> defaultValues) {
        if (defaultValues == null) {
            this.defaultValues.remove(option);
        } else {
            this.defaultValues.put(option, defaultValues);
        }
    }

    @Override
    public void setDefaultSwitch(Option option, Boolean defaultSwitch) {
        if (defaultSwitch == null) {
            this.defaultSwitches.remove(option);
        } else {
            this.defaultSwitches.put(option, defaultSwitch);
        }
    }

    @Override
    public Option getOption(String trigger) {
        return getTriggers().get(new TriggerImpl(trigger));
    }

    protected Map<Trigger, Option> getTriggers() {
        Map<Trigger, Option> triggers = Maps.newHashMap();
        for (Option option : options) {
            triggers.put(new TriggerImpl(option.getName()), option);
            for (Trigger trigger : option.getTriggers()) {
                triggers.put(trigger, option);
            }
        }
        return triggers;
    }

    @Override
    public Boolean getSwitch(Option option, Boolean defaultValue) {
        Boolean value = this.switches.get(option);
        if (value == null) {
            value = defaultValue;
        }
        return value == null ? this.defaultSwitches.get(option) : value;
    }

    @Override
    public List<Object> getOptionValues(Option option) {
        List<Object> values = this.values.get(option);
        return values == null ? Collections.emptyList() : values;
    }

    @Override
    public String getProperty(Option option, String property, String defaultValue) {
        Properties properties = this.properties.get(option);
        String value = properties != null ? properties.getProperty(property, defaultValue) : null;
        return value == null ? defaultValue : value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getValues(Option option, List<T> defaultValues) {
        List<T> values = (List<T>) this.values.get(option);
        if (values == null || values.isEmpty()) {
            values = (List<T>) this.defaultValues.get(option);
        }
        return values == null ? Collections.<T>emptyList() : values;
    }

    @Override
    public String getProperty(String property) {
        return getProperty(new Property(), property);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getProperties(Option option) {
        Properties properties = this.properties.get(option);
        Set keys = properties != null ? properties.keySet() : null;
        return keys != null ? Collections.unmodifiableSet(keys) : Collections.emptySet();
    }

    @Override
    public Set<String> getProperties() {
        return getProperties(new Property());
    }

    @Override
    public boolean isOption(String argument) {
        for (String prefix : root.getPrefixes()) {
            if (argument.startsWith(prefix)) {
                if (current.canProcess(this, argument) || current.findOption(argument) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Set<Option> getOptions() {
        return Collections.unmodifiableSet(options);
    }

    @Override
    public Option getCurrent() {
        return current;
    }

    @Override
    public void setCurrent(Option current) {
        this.current = current;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        for (Iterator iterator = arguments.iterator(); iterator.hasNext(); ) {
            String arg = (String) iterator.next();
            if (arg.indexOf(' ') >= 0) {
                buffer.append("\"").append(arg).append("\"");
            } else {
                buffer.append(arg);
            }
            if (iterator.hasNext()) {
                buffer.append(' ');
            }
        }
        return buffer.toString();
    }
}
