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
package com.nuodb.migration.cli.parse.option;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.nuodb.migration.cli.parse.*;

import java.util.Map;
import java.util.Set;

/**
 * @author Sergey Bushik
 */
public class OptionBuilderImpl implements OptionBuilder {

    private int id;
    private String name;
    private String description;
    private boolean required;
    private Argument argument;
    private Group group;
    private Map<String, OptionFormat> aliases = Maps.newHashMap();
    private Set<Trigger> triggers = Sets.newHashSet();
    private OptionFormat optionFormat;
    private OptionProcessor optionProcessor;

    public OptionBuilderImpl(OptionFormat optionFormat) {
        this.optionFormat = optionFormat;
    }

    @Override
    public OptionBuilder withId(int id) {
        this.id = id;
        return this;
    }

    @Override
    public OptionBuilder withName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public OptionBuilder withAlias(String alias) {
        return withAlias(alias, optionFormat);
    }

    @Override
    public OptionBuilder withAlias(String alias, OptionFormat optionFormat) {
        this.aliases.put(alias, optionFormat);
        return this;
    }

    @Override
    public OptionBuilder withDescription(String description) {
        this.description = description;
        return this;
    }

    @Override
    public OptionBuilder withRequired(boolean required) {
        this.required = required;
        return this;
    }

    public OptionBuilder withGroup(Group group) {
        this.group = group;
        return this;
    }

    @Override
    public OptionBuilder withOptionProcessor(OptionProcessor optionProcessor) {
        this.optionProcessor = optionProcessor;
        return this;
    }

    @Override
    public OptionBuilder withArgument(Argument argument) {
        this.argument = argument;
        return this;
    }

    @Override
    public OptionBuilder withOptionFormat(OptionFormat optionFormat) {
        this.optionFormat = optionFormat;
        return this;
    }

    @Override
    public OptionBuilder withTrigger(Trigger trigger) {
        triggers.add(trigger);
        return this;
    }

    @Override
    public Option build() {
        OptionImpl option = new OptionImpl();
        option.setId(id);
        option.setName(name);
        option.setDescription(description);
        option.setRequired(required);
        option.setOptionProcessor(optionProcessor);
        option.setArgument(argument);
        option.setOptionFormat(optionFormat);
        option.setAliasOptionFormats(aliases);
        option.setGroup(group);
        for (Trigger trigger : triggers) {
            option.addTrigger(trigger);
        }
        return option;
    }
}