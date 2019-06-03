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
package com.nuodb.migrator.cli.parse.parser;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionSet;

import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Iterables.tryFind;

/**
 * Instances of CommandLine represent a executable line that has been processed
 * according to the spec supplied to the parser.
 */
public abstract class OptionSetImpl implements OptionSet {

    @Override
    public boolean hasOption(String trigger) {
        return getOption(trigger) != null;
    }

    @Override
    public Option getOption(final int id) {
        final Optional<Option> optional = tryFind(getOptions(), new Predicate<Option>() {
            @Override
            public boolean apply(Option option) {
                return option.getId() == id;
            }
        });
        return optional.isPresent() ? optional.get() : null;
    }

    @Override
    public <T> List<T> getValues(String trigger) {
        return getValues(getOption(trigger), Collections.<T>emptyList());
    }

    @Override
    public <T> List<T> getValues(String trigger, List<T> defaultValues) {
        return getValues(getOption(trigger), defaultValues);
    }

    @Override
    public <T> List<T> getValues(Option option) {
        return getValues(option, Collections.<T>emptyList());
    }

    @Override
    public Object getValue(String trigger) {
        return getValue(getOption(trigger), null);
    }

    @Override
    public Object getValue(String trigger, Object defaultValue) {
        return getValue(getOption(trigger), defaultValue);
    }

    @Override
    public Object getValue(Option option) {
        return getValue(option, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getValue(Option option, Object defaultValue) {
        List<Object> values;
        if (defaultValue == null) {
            values = getValues(option);
        } else {
            values = getValues(option, Collections.singletonList(defaultValue));
        }
        if (values.isEmpty()) {
            return defaultValue;
        }
        return values.get(0);
    }

    @Override
    public Boolean getSwitch(String trigger) {
        return getSwitch(getOption(trigger), null);
    }

    @Override
    public Boolean getSwitch(String trigger, Boolean defaultValue) {
        return getSwitch(getOption(trigger), defaultValue);
    }

    @Override
    public Boolean getSwitch(Option option) {
        return getSwitch(option, null);
    }

    @Override
    public String getProperty(Option option, String property) {
        return getProperty(option, property, null);
    }

    @Override
    public int getOptionCount(String trigger) {
        return getOptionCount(getOption(trigger));
    }

    @Override
    public int getOptionCount(Option option) {
        if (option == null) {
            return 0;
        }
        int count = 0;
        for (Option o : getOptions()) {
            if (option.equals(o)) {
                ++count;
            }
        }
        return count;
    }
}
